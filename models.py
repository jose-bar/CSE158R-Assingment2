from datetime import date, datetime
from surprise import SVD, SVDpp, Dataset as SDataset, Reader as SReader
from surprise import accuracy
import pandas as pd
import json
import gzip

def readGz(path, count = None):
  i = 0
  for l in gzip.open(path, 'rt'):
    if count and i >= count:
      return
    yield eval(l)
    i += 1


def to_date(date_str: str, date_format_string: str = "%Y-%m-%d"):
    return datetime.strptime(date_str, date_format_string)
    

def gen_feature_reviews(l):
    datum = {
        "user_id": hash(l["username"]),
        "hours": l["hours"] if "hours" in l else 0,
        "product_id": int(l["product_id"]),
        "date": to_date(l["date"]),
        "early_access": int(l["early_access"]),
        "is_review": int(len(l["text"]) > 0)
    }

    return datum

# {u'publisher': u'INGAME', u'genres': [u'Indie', u'RPG'], u'app_name': u'Bravium', u'sentiment': u'3 user reviews', u'title': u'Bravium', u'url': u'http://store.steampowered.com/app/747320/Bravium/', u'release_date': u'2018-01-04', u'tags': [u'Indie', u'RPG', u'Puzzle', u'Tower Defense'], u'discount_price': 11.99, u'reviews_url': u'http://steamcommunity.com/app/747320/reviews/?browsefilter=mostrecent&p=1', u'specs': [u'Single-player', u'Steam Achievements', u'Steam Leaderboards'], u'price': 14.99, u'early_access': False, u'id': u'747320', u'developer': u'INGAME'}
def gen_feature_games(l):
  datum = None


  if "id" in l:
    datum = {
        "product_id": int(l['id']),
        "publisher": l['publisher'] if "publisher" in l else "",
        "developer": l['developer'] if "developer" in l else "",
        "title": l['title'] if "title" in l else "",
        "price": l['price'] if "price" in l and type(l['price']) == float else 0.0
    }

  return datum

def generate_dataset(gen, functor):
    rows = []

    for l in gen:
        d = functor(l)
        if d is None:
            continue
        rows.append(d)
        
    df = pd.DataFrame(rows)
    return df


# datasets
# steam_reviews.json.gz
# steam_games.json.gz

reviews_df = generate_dataset(readGz("datasets/steam_reviews.json.gz", count=1000), gen_feature_reviews)
games_df = generate_dataset(readGz("datasets/steam_games.json.gz"), gen_feature_games)
combined_df = reviews_df.merge(games_df, on="product_id", how="inner")


print(reviews_df[:5])
print(games_df[:5])
print(combined_df[:5])
