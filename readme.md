# scrape_em.py

Scrape data from a site that is difficult to use. This is written for a very specific purpose and will not work as a general solution.

## Dependencies

1. python2.7
2. requests `pip install requests` 
3. beautifulsoup `pip install bs4`

## Usage

`python scrape_em.py -h` for full options

## Examples

`python scrape_em.py --url 'https://myfavoritesite.com' --max 5 --csv 5_results --load data.json`

## To Do

1. give option for config file definition of args. cmd line can get crowded quickly, especially with long urls

