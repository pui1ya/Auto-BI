#basically to group the products customers will obviously buy (recommendation system haha) ✋😆

#takes transactions and products table 

import pandas as pd
import numpy as np

def build_basket(transactions):
    return transactions[['order_id', 'product_id']]


#to see how many times product has been bought
def compute_product_appearance(basket):

    total_orders = basket['order_id'].nunique()

    support = (basket.groupby("product_id").agg(
        order_count = ("order_id", "nunique")
    ).reset_index())

    support["support"] = (support['order_count']/total_orders)*10000

    return support


#generating product pairs
def generate_product_pairs(basket):

    #to generate combination of products
    pairs = (basket.merge(basket, on='order_id'))
    pairs = pairs[pairs["product_id_x"] != pairs["product_id_y"]]

    return pairs[['order_id', 'product_id_x', 'product_id_y']]


#to compute most occuring product pairs
def most_occuring_pairs(basket, pairs):
    pairs_support = (pairs.groupby(['product_id_x', 'product_id_y']).agg(
        pair_order = ("order_id", "nunique")
    ).reset_index())

    total_orders = basket["order_id"].nunique()

    pairs_support['support'] = (pairs_support['pair_order'] / total_orders)*10000

    return pairs_support


#now comes the confidence 🗿
def compute_confidence(pairs_support, product):
    df = pairs_support.merge(product[['product_id', 'order_count']], 
                             left_on = 'product_id_x',
                             right_on = 'product_id',
                             how = "left")
    
    df['confidence'] = (df['pair_order']/df['order_count'])*100

    return df


def top_associations(df, min_support, min_confidence):
    return (
        df[
            (df["support"] >= min_support) &
            (df["confidence"] >= min_confidence)
        ]
        .sort_values(
            ["confidence", "support"],
            ascending=False
        ).reset_index(drop=True)
    )

# for gods sake i'm doing soooooo wrong in this function. ig i'll just comment it out 🥹
# def add_product_names(df, products):

    # df = df.merge(
    #     products[["product_id", "product_name"]],
    #     left_on="product_id_x",
    #     right_on="product_id",
    #     how="left"
    # )

    # df = df.rename(columns={"product_name": "product_name_x"})
    # df = df.drop(columns=["product_id"])

    # df = df.merge(
    #     products[["product_id", "product_name"]],
    #     left_on="product_id_y",
    #     right_on="product_id",
    #     how="left"
    # )

    # df = df.rename(columns={"product_name": "product_name_y"})
    # df = df.drop(columns=["product_id"])

    # return df



if __name__ == "__main__":
    from app.etl.transform import table_builder
    from app.etl.clean import clean_raw_data
    from app.etl.ingest import ingest_csv

    file = '/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv'

    ingest = ingest_csv(file)
    clean = clean_raw_data(ingest)

    builder = table_builder(clean)

    transactions = builder.build_transactions_table()

    basket = build_basket(transactions)

    print("basket")
    print(basket)

    print("product appearances")
    print(compute_product_appearance(basket))

    print("product pairs")
    print(generate_product_pairs(basket))

    pairs = generate_product_pairs(basket)
    print("most occuring pairs")
    print(most_occuring_pairs(basket, pairs))

    pairs_support = most_occuring_pairs(basket, pairs)
    product = compute_product_appearance(basket)
    print("compute confidence")
    print(compute_confidence(pairs_support, product))

    df = compute_confidence(pairs_support, product)
    print("df with confidence")
    print(df)

    min_support = df["support"].quantile(0.95)
    min_confidence = df["confidence"].quantile(0.9)

    print(top_associations(df, min_support, min_confidence))

