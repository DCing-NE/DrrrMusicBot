# music_sources/qq_music.py
"""QQ 音乐解析器（仅元数据）。

- URL → songmid / disstid（长链 + 短链跟随）
- songmid → 官方元数据（歌名/歌手/时长），走 musicu.fcg
- 歌单走 fcg_ucc_getcdinfo_byids_cp.fcg

官方接口返回的 vkey 直链依赖登录态，在 drrr 房间不可用，
因此本模块**不负责取播放直链**。播放由 music_bot 走 gequbao 搜索。

不继承 Module、不碰队列、不注册命令。
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiohttp

PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

QQ_HEADERS = {
    "User-Agent": PC_UA,
    "Referer": "https://y.qq.com/",
    "Accept": "application/json, text/plain, */*",
}

QQ_MUSICU = "https://u.y.qq.com/cgi-bin/musicu.fcg"
QQ_PLAYLIST = (
    "https://i.y.qq.com/qzone-music/fcg-bin/"
    "fcg_ucc_getcdinfo_byids_cp.fcg"
)


class QQMusicSource:
    def __init__(self, *, logger, session_getter=None):
        self._logger = logger
        self._session_getter = session_getter

    @asynccontextmanager
    async def _session_ctx(self):
        if self._session_getter:
            yield self._session_getter()
        else:
            s = aiohttp.ClientSession()
            try:
                yield s
            finally:
                await s.close()

    @staticmethod
    def is_qq_url(url: str) -> bool:
        host = urlparse(url).netloc.lower()
        return host == "y.qq.com" or host.endswith(".y.qq.com")

    async def _resolve_short_link(self, url: str) -> str | None:
        headers = {"User-Agent": PC_UA, "Accept-Language": "zh-CN,zh;q=0.9"}
        try:
            async with self._session_ctx() as session:
                async with session.get(
                    url, allow_redirects=True, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    final = str(resp.url)
                    if final != url:
                        return final
        except Exception as e:
            self._logger.warning(
                "[QQMusic] 短链解析失败: %s, error: %s", url, e
            )
        return None

    @staticmethod
    def _parse_qq_url(url: str) -> tuple[str, str] | None:
        """从 URL 中提取 (类型, ID)。

        支持的格式：
        - https://y.qq.com/n/ryqq/songDetail/{songmid}
        - https://y.qq.com/n/ryqq/playlist/{disstid}
        - https://i.y.qq.com/v8/playsong.html?songmid={songmid}
        - https://i2.y.qq.com/n3/.../playlist.html?id={disstid}
        - https://y.qq.com/?songmid={songmid}
        - https://y.qq.com/?disstid={disstid}
        - https://y.qq.com/...?id={数字}（按纯数字判歌单，字母数字判单曲）
        """
        parsed = urlparse(url)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if m := re.search(r"/songDetail/([A-Za-z0-9]+)", path):
            return ("song", m.group(1))
        if m := re.search(r"/playlist/(\d+)", path):
            return ("playlist", m.group(1))

        for key in ("songmid", "songMid", "songMID"):
            if key in qs and qs[key][0]:
                return ("song", qs[key][0])
        for key in ("disstid", "playlistId", "dissid"):
            if key in qs and qs[key][0]:
                return ("playlist", qs[key][0])

        if "id" in qs and qs["id"][0]:
            ident = qs["id"][0]
            url_lower = url.lower()
            if "playlist" in url_lower or "taoge" in url_lower:
                return ("playlist", ident)
            if "song" in url_lower or "playsong" in url_lower:
                return ("song", ident)
            return ("playlist" if ident.isdigit() else "song", ident)

        return None

    async def resolve(self, url: str) -> dict[str, Any] | None:
        """URL → 元数据。短链自动跟随。"""
        parsed = self._parse_qq_url(url)

        if not parsed and self.is_qq_url(url):
            resolved = await self._resolve_short_link(url)
            if resolved:
                self._logger.info("[QQMusic] 短链解析: %s", resolved)
                url = resolved
                parsed = self._parse_qq_url(url)

        if not parsed:
            self._logger.warning("[QQMusic] 无法识别的QQ音乐链接: %s", url)
            return None

        link_type, ident = parsed
        if link_type == "song":
            return await self._resolve_song(ident)
        return await self._resolve_playlist(ident)

    async def _musicu_call(self, payload: dict) -> dict | None:
        try:
            async with self._session_ctx() as session:
                async with session.post(
                    QQ_MUSICU, json=payload,
                    headers={**QQ_HEADERS, "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        self._logger.warning(
                            "[QQMusic] musicu HTTP %s", resp.status
                        )
                        return None
                    return await resp.json(content_type=None)
        except Exception as e:
            self._logger.warning("[QQMusic] musicu 请求异常: %s", e)
            return None

    async def _resolve_song(self, songmid: str) -> dict[str, Any]:
        data = await self._musicu_call({
            "comm": {"uin": "0", "format": "json", "ct": 24, "cv": 0},
            "req_1": {
                "module": "music.pf_song_detail_svr",
                "method": "get_song_detail",
                "param": {"song_mid": songmid},
            },
        })
        track = self._extract_track(data) if data else None
        if not track:
            return self._make_song_dict(songmid, None, 0, songmid)
        return self._make_song_dict(
            track["name"], track["artist"],
            track["duration_ms"], songmid,
        )

    @classmethod
    def _extract_track(cls, data: Any) -> dict[str, Any] | None:
        if not isinstance(data, dict):
            return None
        track = (
            ((data.get("req_1") or {}).get("data") or {}).get("track_info")
            or ((data.get("data") or {}).get("track_info"))
            or {}
        )
        if not track:
            return None
        return {
            "name": track.get("name") or track.get("title"),
            "artist": cls._join_artists(track.get("singer")),
            "duration_ms": int(track.get("interval") or 0) * 1000,
        }

    async def _resolve_playlist(self, disstid: str) -> dict[str, Any] | None:
        url = (
            f"{QQ_PLAYLIST}?type=1&json=1&utf8=1&onlysong=0&nosign=1"
            f"&disstid={disstid}&g_tk=5381&loginUin=0&hostUin=0"
            f"&format=json&inCharset=GB2312&outCharset=utf-8"
            f"&notice=0&platform=yqq&needNewCode=0"
        )
        try:
            async with self._session_ctx() as session:
                async with session.get(
                    url, headers=QQ_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        self._logger.warning(
                            "[QQMusic] 歌单 HTTP %s, disstid=%s",
                            resp.status, disstid,
                        )
                        return None
                    data = await resp.json(content_type=None)
        except Exception as e:
            self._logger.warning(
                "[QQMusic] 歌单请求异常: %s, disstid=%s", e, disstid
            )
            return None

        cdlist = (data or {}).get("cdlist") or []
        if not cdlist:
            return None
        cd = cdlist[0]
        songlist = cd.get("songlist") or []
        if not songlist:
            return None

        songs = []
        for s in songlist:
            mid = s.get("songmid") or s.get("mid")
            if not mid:
                continue
            songs.append(self._make_song_dict(
                s.get("songname") or s.get("name"),
                self._join_artists(s.get("singer")),
                int(s.get("interval") or 0) * 1000,
                mid,
            ))

        return {
            "playlist_id": disstid,
            "playlist_name": cd.get("dissname") or "",
            "songs": songs,
        }

    @staticmethod
    def _make_song_dict(
        name: str | None, artist: str | None,
        duration_ms: int, mid: str,
    ) -> dict[str, Any]:
        return {
            "name": name or mid,
            "artist": artist or None,
            "duration_ms": duration_ms,
            "qq_mid": mid,
        }

    @staticmethod
    def _join_artists(singers: Any) -> str | None:
        """拼接歌手名（多歌手用 / 分隔）。"""
        if not isinstance(singers, list):
            return None
        return "/".join(
            s.get("name", "") for s in singers
            if isinstance(s, dict) and s.get("name")
        ) or None