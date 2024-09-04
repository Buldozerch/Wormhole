import random
from loguru import logger

from faker import Faker
import asyncio
from asyncio import Semaphore
import aiohttp

fake = Faker()

proxy_file = 'proxy.txt'
with open(proxy_file, 'w+') as file:
    proxys = file.read().split('\n')
email_file = 'emails.txt'
with open(email_file, 'w+') as file:
    emails = file.read().split('\n')
logger.add(
    f'{"debug.log"}',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}',
    level='WARNING'
)


def parse_proxy():
    proxy = random.choice(proxys)
    for row in proxy.split('\n'):
        if row.startswith('http'):
            return row
        if "@" in row and "http" not in row:
            return "http://" + row
        value = row.strip().split(':')
        ip = value[0]
        port = value[1]
        login = value[2]
        password = value[3]
        return f'http://{login}:{password}@{ip}:{port}'


async def response(email, semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            headers = {
                'authority': 'sigma.wormhole.com',
                'accept': '*/*',
                'content-type': 'application/json',
                'origin': 'https://sigma.wormhole.com',
                'referer': 'https://sigma.wormhole.com/',
                'sec-ch-ua-mobile': '?0',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': f'{fake.user_agent()}',
            }
            json_data = {
                'emailAddress': f'{email}',
                'firstName': f'{fake.first_name()}',
                'keepInformed': True,
            }
            wronger = 0
            while wronger <= 10:
                proxy = parse_proxy()
                async with session.post(
                        'https://sigma.wormhole.com/api/airtable',
                        headers=headers, json=json_data, proxy=proxy) as responser:
                    if responser.status == 201:
                        return logger.success(f'Success send form from {email}')
                    else:
                        await asyncio.sleep(5)
                        wronger += 1
                    if wronger == 10:
                        logger.error(f'Wrong send form from {email} start try {wronger + 1}')


async def main():
    semaphore: Semaphore = Semaphore(25)
    tasks = [asyncio.create_task(response(email.strip().split(':')[0], semaphore=semaphore)) for email in emails]
    while tasks:
        # Запуск и ожидание завершения задач по мере их выполнения
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        tasks = list(pending)  # Обновление списка оставшихся задач

        # Обработка завершенных задач
        for task in done:
            try:
                await task  # Обработка результата задачи
            except Exception as e:

                logger.error(f"Error in task: {e}")
    logger.success('Work done')

if __name__ == "__main__":
    asyncio.run(main())