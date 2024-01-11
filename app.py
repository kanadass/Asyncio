import asyncio
import aiohttp
import datetime
from more_itertools import chunked

from models import init_db, Session, SwapiPeople


MAX_CHUNK = 5

async def get_title_and_name(data, session):
    async def process_urls(url):
        response = await session.get(url)
        json_data = await response.json()
        if 'title' in json_data:
            return json_data['title']
        else:
            return json_data['name']

    if 'films' in data:
        titles = await asyncio.gather(*(process_urls(url) for url in data['films']))
        data['films'] = ', '.join(titles)

    for category in ['species', 'starships', 'vehicles']:
        if category in data:
            names = await asyncio.gather(*(process_urls(url) for url in data[category]))
            data[category] = ', '.join(names)

async def get_person(person_id, session):
    http_response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_data = await http_response.json()
    await get_title_and_name(json_data, session)
    return json_data

async def insert_records(records, session):
    records = [SwapiPeople(
        name=record.get('name'),
        birth_year=record.get('birth_year'),
        eye_color=record.get('eye_color'),
        films=record.get('films', None),
        gender=record.get('gender'),
        hair_color=record.get('hair_color'),
        height=record.get('height'),
        homeworld=record.get('homeworld'),
        mass=record.get('mass'),
        skin_color=record.get('skin_color'),
        species=record.get('species', None),
        starships=record.get('starships', None),
        vehicles=record.get('vehicles', None)
    ) for record in records]

    async with Session() as db_session:
        db_session.add_all(records)
        await db_session.commit()

async def main():
    await init_db()
    session = aiohttp.ClientSession()

    for people_id_chunk in chunked(range(1, 90), MAX_CHUNK):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_records(result, session))

    await session.close()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)