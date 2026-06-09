import luigi
import requests
import requests_cache
import pandas as pd
from bs4 import BeautifulSoup
from pkb.config import settings, logger
from pkb.tasks.base import BaseTask


class IndexHerbariumTask(BaseTask):

    input_dir = settings.input_dir / "IndexHerbarium"


    limit_per_page = 1000

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "index_herbariorum" / "herbarium-list.parquet"
        )

    
    def run(self):

        rows = []

        html_files = list(self.input_dir.glob("*.html"))

        for html_file in html_files:
            html = html_file.read_text(encoding="utf-8")
            page_rows = self._parse_html_page(html)
            rows.extend(page_rows)

        df = pd.DataFrame(rows)

        self.output().makedirs()
        df.to_parquet(self.output().path, index=False)

        logger.info(
            "Parsed %s herbarium records from %s HTML files",
            len(df),
            len(html_files),
        )        



    def _parse_html_page(self, html):

        headers = ['herbariumCode', 'Institution', 'Location']
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        table = soup.select_one("table.table-results")

        if table is None:
            raise RuntimeError("No results table found")

        for tr in table.find_all("tr"):
            irn = tr.get("data-imu-irn")
            cells = tr.find_all("td")
            if not cells:
                continue

            values = [td.get_text(" ", strip=True) for td in cells]
            row = dict(zip(headers, values))    
            row['IRN'] = irn

            rows.append(row)

        return rows





if __name__ == "__main__":
    luigi.build([IndexHerbariumTask(force=True)], local_scheduler=True)