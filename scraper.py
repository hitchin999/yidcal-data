import asyncio
import json
import aiohttp
from bs4 import BeautifulSoup
import unicodedata
from datetime import datetime
import os
import logging

_LOGGER = logging.getLogger(__name__)

def remove_nikud(text: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def hebrew_to_int(hebrew: str) -> int:
    if not hebrew:
        return 0
    hebrew = hebrew.replace("'", "").replace('"', "")
    letters = {'א': 1, 'ב': 2, 'ג': 3, 'ד': 4, 'ה': 5, 'ו': 6, 'ז': 7, 'ח': 8, 'ט': 9, 'י': 10,
               'כ': 20, 'ל': 30, 'מ': 40, 'נ': 50, 'ס': 60, 'ע': 70, 'פ': 80, 'צ': 90,
               'ק': 100, 'ר': 200, 'ש': 300, 'ת': 400}
    value = 0
    for letter in hebrew:
        value += letters.get(letter, 0)
    return value

month_map = {
    "ניסן": 1,
    "אייר": 2,
    "סיון": 3,
    "תמוז": 4,
    "אב": 5,
    "מנחם אב": 5,
    "מנ\"א": 5,
    "אלול": 6,
    "תשרי": 7,
    "חשון": 8,
    "מרחשון": 8,
    "כסלו": 9,
    "טבת": 10,
    "שבט": 11,
    "אדר": 12,
    "אדר א'": 12,
    "אדר ב'": 13,
}

async def fetch_forum_page(url: str) -> dict[tuple[int, int], list[dict]]:
    local_yahrtzeits = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10, ssl=False) as response:
                if response.status != 200:
                    _LOGGER.warning(f"Failed to fetch {url}: {response.status}")
                    return local_yahrtzeits
                text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        posts = soup.find_all('div', class_='post has-profile bg2')
        for post in posts:
            author_elem = post.find('dl', class_='postprofile')
            if author_elem:
                author = author_elem.find('a', class_='username')
                if author and author.text.strip() != 'אלטערנעסייד פארקינג':
                    continue
            content_div = post.find('div', class_='postbody').find('div', class_='content')
            if not content_div:
                continue
            content = content_div.get_text(separator='\n').strip().split('\n\n')
            current_day = None
            current_month = None
            default_pref = None
            for block in content:
                block_lines = [line.strip() for line in block.split('\n') if line.strip()]
                for i, line in enumerate(block_lines):
                    line = line.replace('\\', '').replace('\'', '')
                    if (line and any(c in 'אבגדהוזחטיכל' for c in line[:2]) and any(month in line for month in month_map) and not line.startswith('רבי ')):
                        parts = line.split(' ')
                        day_text = parts[0]
                        month_text = next((m for m in sorted(month_map.keys(), key=len, reverse=True) if m in line), None)
                        if not month_text:
                            current_day = None
                            continue
                        current_month = month_map.get(month_text)
                        if current_month is None:
                            current_day = None
                            continue
                        if current_month in [12, 13]:
                            current_month = 12
                            if month_text == "אדר א'":
                                default_pref = 1
                            elif month_text == "אדר ב'":
                                default_pref = 2
                            else:
                                default_pref = None
                        else:
                            default_pref = None
                        try:
                            current_day = hebrew_to_int(day_text)
                        except ValueError:
                            current_day = None
                            continue
                    elif current_day and (line.startswith('רבי ') or line.startswith('יששכר ') or line.startswith('השר ') or line.startswith('שמעון ') or line.startswith('??רבי ') or line.startswith('?רבי ') or line.startswith('משה ') or line.startswith('רבינו ')):
                        line = line.replace('??', '')
                        adar_pref = default_pref
                        if line.startswith('(א)'):
                            adar_pref = 1
                            line = line[3:].strip()
                        elif line.startswith('(ב)'):
                            adar_pref = 2
                            line = line[3:].strip()
                        d = remove_nikud(line)
                        key = (current_month, current_day)
                        entry_data = {'text': d, 'adar_pref': adar_pref}
                        if key not in local_yahrtzeits:
                            local_yahrtzeits[key] = []
                        local_yahrtzeits[key].append(entry_data)
                    elif line in ('ובנו', 'וחתנו'):
                        continue
        return local_yahrtzeits
    except Exception as e:
        _LOGGER.error(f"Error fetching/parsing {url}: {e}")
        return local_yahrtzeits

async def main():
    base_url = "https://forum.yidtish.com/viewtopic.php?t=803&start="
    pages = range(0, 300, 25)
    tasks = [fetch_forum_page(base_url + str(start)) for start in pages]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    data = {}
    for result in results:
        if not isinstance(result, Exception):
            for key, value in result.items():
                if key not in data:
                    data[key] = []
                data[key].extend(value)
    # Dedup preserving order
    for key in data:
        seen = set()
        ordered_unique = []
        for e in data[key]:
            tup = (e['text'], e['adar_pref'])
            if tup not in seen:
                seen.add(tup)
                ordered_unique.append(e)
        data[key] = ordered_unique
    with open('new_yahrtzeit_cache.json', 'w', encoding='utf-8') as f:
        json.dump({f"{k[0]}_{k[1]}": v for k, v in data.items()}, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    asyncio.run(main())
