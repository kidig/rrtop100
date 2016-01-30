# coding: utf-8
import os
import logging
import click
import asyncio
import aiohttp
import lxml.html
from urllib.parse import unquote

try:
    from asyncio import JoinableQueue as Queue
except ImportError:
    from asyncio import Queue


LOGGER = logging.getLogger(__name__)

RADIORECORD_TRACKLIST = 'http://www.radiorecord.ru/xml/top100/'


def filename_from_url(url):
    return os.path.basename(unquote(url))


class RecordTracksCrawler:
    def __init__(self, station_url, output_dir='data/', loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.max_tasks = 8
        self.q = Queue()
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.station_url = station_url
        self.output_dir = output_dir

    def close(self):
        self.session.close()

    def parse_tracks(self, content):
        doc = lxml.html.document_fromstring(content)
        for track in doc.xpath('//div[@class="top100_media"]//a'):
            url = track.attrib['href'].strip()
            # LOGGER.debug('url href: %r', url)
            # url = url.split('download.php?url=')[1]

            if url:
                yield url

    @asyncio.coroutine
    def get_tracks(self):
        r = yield from self.session.get(self.station_url)
        
        if not r.status == 200:
            LOGGER.error('cannot get tracks from %r', self.station_url)
            return

        try:
            content = yield from r.read()
            
            c = 1
            for url in self.parse_tracks(content):
                self.q.put_nowait((url, c))
                c += 1

            LOGGER.info('got %d tracks from %r', c-1, self.station_url)

        finally:
            yield from r.release()

    @asyncio.coroutine
    def crawl(self):
        yield from self.get_tracks()

        workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_tasks)]

        yield from self.q.join()

        for w in workers:
            w.cancel()

    @asyncio.coroutine
    def work(self):
        try:
            while True:
                url, num = yield from self.q.get()

                LOGGER.info("start #%d %r", num, filename_from_url(url))
                
                yield from self.download(url)
                self.q.task_done()

                LOGGER.info("finish #%d", num)

        except asyncio.CancelledError:
            pass

    @asyncio.coroutine
    def download(self, url):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        r = yield from self.session.get(url)
        if not r.status == 200:
            LOGGER.error('cannot download track from %r', url)
            return

        try:
            filename = os.path.join(self.output_dir, filename_from_url(url))
            with open(filename, 'wb') as fp:
                while True:
                    chunk = yield from r.content.read(1024)
                    if not chunk:
                        break
                    fp.write(chunk)

        finally:
            yield from r.release()


@click.command()
@click.argument('station', required=1)
@click.option('--output', default='data/', type=click.Path(), help='Output directory to downloaded files')
@click.option('-v', '--verbose', default=False, is_flag=True, help='Increase output verbosity')
def main(station, output, verbose):
    """This script will download top-100 track for selected <STATION>."""
    if verbose:
        logging.basicConfig(level=logging.INFO)

    station_url = "%s%s.txt" % (RADIORECORD_TRACKLIST, station)
    loop = asyncio.get_event_loop()
    crawler = RecordTracksCrawler(station_url, output)

    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupted\n')
    finally:
        crawler.close()

        # next two lines are required for actual aiohttp resource cleanup
        loop.stop()
        loop.run_forever()

        loop.close()

if __name__ == '__main__':
    main()
