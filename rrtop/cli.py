import os
import asyncio
import aiohttp
import click
import lxml.html
from urllib.parse import unquote

RADIORECORD_URL = 'http://www.radiorecord.ru/xml/top100/'


def parse_tracks(content):
    doc = lxml.html.document_fromstring(content)
    for track in doc.xpath('//div[@class="top100_media"]//a'):
        url = track.attrib['href'].strip()
        if url:
            yield url


async def get_tracks(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if not resp.status == 200:
                print("Cannot get tracks: {}".format(url))
                return

            return await resp.read()


async def handle_track(task_id, queue, **kwargs):
    while not queue.empty():
        track_id, url = await queue.get()
        print(task_id, track_id, url)
        try:
            await download_track(url, **kwargs)
        except (aiohttp.ServerDisconnectedError) as e:
            print(task_id, "Server disconnected: {}".format(str(e)))
            queue.put_nowait((track_id, url))
        else:
            print(task_id, 'done')


async def download_track(url, **kwargs):
    output_dir = kwargs.pop('output_dir')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if not resp.status == 200:
                    print("Cannot download track: {}".format(url))
                    return

                filename = os.path.join(output_dir, os.path.basename(unquote(url)))
                with open(filename, 'wb') as fp:
                    while True:
                        chunk = await resp.content.read(1024)
                        if not chunk:
                            break
                        fp.write(chunk)
    except (aiohttp.ServerDisconnectedError) as e:
        print("Error: {}".format(str(e)))
        return


@click.command()
@click.argument('station', required=True)
@click.option('--output', default='data/', type=click.Path(), help='Output directory')
@click.option('--threads', default=5, type=click.INT, help='Number or concurrent threads (Default: 5)')
def main(station, output, threads):
    """RadioRecord Top-100 Hits Download from STATION"""
    url = '{}{}.txt'.format(RADIORECORD_URL, station)
    loop = asyncio.get_event_loop()
    content = loop.run_until_complete(get_tracks(url))

    if content:
        q = asyncio.Queue()

        track_id = 1
        for url in parse_tracks(content):
            q.put_nowait((track_id, url))
            track_id += 1

        tasks = [handle_track(task_id, q, output_dir=output) for task_id in range(threads)]
        loop.run_until_complete(asyncio.wait(tasks))

    loop.close()