import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    from io import BytesIO
    import pathlib
    import zipfile

    import bs4
    import httpx
    import polars as pl
    import datetime as dt
    return BytesIO, dt, httpx, pl, zipfile


@app.cell
def _(httpx):
    tripdata_url = 'https://s3.amazonaws.com/hubway-data/201906-bluebikes-tripdata.zip'

    tripdata_r = httpx.get(
        url=tripdata_url,
    )
    tripdata_r.status_code
    return (tripdata_r,)


@app.cell
def _(BytesIO, tripdata_r, zipfile):
    czip = zipfile.ZipFile(BytesIO(tripdata_r.content))
    return (czip,)


@app.cell
def _(czip):
    list(czip.namelist())
    return


@app.cell
def _(czip, pl):
    pl.read_csv(czip.read('202506-bluebikes-tripdata.csv'))
    return


@app.cell
def _(dt):
    def retrieve_urls(start_M = 1, start_Y = 2015, end_M = 5, end_Y = 2025):
        #use parameters to make date variables
        start_date = dt.date(start_Y, start_M, 1)
        end_date = dt.date(end_Y, end_M, 1)

        # generate list of months between start and end
        dates = []
        year, month = start_date.year, start_date.month
        while (year < end_date.year) or (year == end_date.year and month <= end_date.month):
            dates.append(dt.date(year, month, 1))
            month += 1
            if month > 12:
                month = 1
                year += 1

        predates = [date for date in dates if date<dt.date(2018,5,1)]
        postdates = [date for date in dates if date>=dt.date(2018,5,1)]

        preurls = [f'https://s3.amazonaws.com/hubway-data/{date.year}{date.strftime("%m")}-hubway-tripdata.zip' for date in predates]
        posturls = [f'https://s3.amazonaws.com/hubway-data/{date.year}{date.strftime("%m")}-bluebikes-tripdata.zip' for date in postdates]

        urls = preurls + posturls

        # Convert to Polars Series if needed
        #print(urls)
        return {date: url for (date, url) in zip(dates, urls)} # Returns a Python list of datetime.date
    return (retrieve_urls,)


@app.cell
def _(BytesIO, httpx, pl, zipfile):
    def get_requests(urls):
       dates = [date for date in urls.keys()] 
       czips = [zipfile.ZipFile(BytesIO(httpx.get(url=url).content)) for url in urls.values()] 
       dfs = [pl.read_csv(czip.read(list(czip.namelist())[0]), 
                         schema_overrides={
                             'birth year' : pl.String
                         }) for czip in czips] 

       return {date:(czip, df) for (date, czip, df) in zip(dates, czips, dfs)}
    return (get_requests,)


@app.cell
def _(get_requests, retrieve_urls):
    def zip_dict(start_M = 1, start_Y = 2015, end_M = 5, end_Y = 2025):
        urls = retrieve_urls(start_M, start_Y,end_M, end_Y)
        dict = get_requests(urls)

        return dict
    return (zip_dict,)


app._unparsable_cell(
    r"""
    def same_schema([])
    """,
    name="_"
)


@app.cell
def _(zip_dict):
    dict = zip_dict(7, 2015, 9, 2015)
    return (dict,)


@app.cell
def _(dict):
    dict
    return


app._unparsable_cell(
    r"""
    def knit_dfs()
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
