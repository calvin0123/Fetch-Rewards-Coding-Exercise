"""
File Name: dqcheck.py
Author: Calvin Chen
------------------------------
This file reads in the JSON file,
covert to dataframe, and support 
DQ quality check.
"""

import json
import csv
import io
from zipfile import ZipFile
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import re


def nullCheck(df, title):
    """
    do the null check
    """
    df = round(df.isnull().sum() / len(df) * 100, 2).sort_values(ascending=True)
    print(df)
    print("==================================")
    
    plt.figure(figsize=(8, 6))
    df.plot.barh()
    plt.xlabel("Null Percentage (%)")
    plt.ylabel(title)
    plt.title(f"Null percentage of {title}")
    plt.xlim(0, 100)
    plt.figure(figsize=(8, 6))
    
    plt.show()

    
def duplicateCheck(df):
    
    return df.duplicated().sum()


def preprocess(df, dateColumns):    
    """
    this function preprocesses the data
    1. convert timestamp data into datetime 
    2. get the id for that particular table
    
    # note: value might be none, and the type in float, so we need to clean this up
    datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    """
    # id
    df['_id'] = df['_id'].apply(lambda x: x['$oid'])

    # date time
    if dateColumns:
        for col in dateColumns:        
            df[col] = df[col].apply(lambda x: datetime.fromtimestamp(x['$date'] / 1000) if type(x) != float else None)
    return df
    
    
def create_item_df(receipt_df):
    """
    this fuction creates item_df using both pandas package (slow)
    if can provide the attributes of all the item list, we might speed up the processing time
    
    TODO: Fix the issse, not enough column
    """
    
    # not sure how many list, if get the list can add to this dict
#     item_dict = {
#         "_id": [],
#         "barcode": [],
#         "description": [],
#         "finalPrice": [],
#         "itemPrice": [],
#         "needsFetchReview": [],
#         "partnerItemId": [],
#         "preventTargetGapPoints": [],
#         "quantityPurchased": [],
#         "userFlaggedBarcode": [],
#         "userFlaggedNewItem": [],
#         "userFlaggedPrice": [],
#         "userFlaggedQuantity": [],
#         "originalMetaBriteBarcode": []
# }
    
#     item_temp_df = receipt_df[['_id', 'rewardsReceiptItemList']]
    
#     for i in range(len(item_temp_df['rewardsReceiptItemList'])):
#         receipt_id = item_temp_df['_id'].iloc[i]
#         if type(item_temp_df['rewardsReceiptItemList'].iloc[i]) != float:
#             for d in item_temp_df['rewardsReceiptItemList'].iloc[i]:

#                 item_dict['_id'].append(receipt_id)
#                 for key, val in item_dict.items():
#                     if key == '_id':
#                         continue
#                     else:
#                         if key in d.keys():
#                             item_dict[key].append(d[key])
#                         else:
#                             item_dict[key].append(None)
    
#     item_df = pd.DataFrame(item_dict)
    
    
    # ========
    
    # This methods take too long too run .. but can get all the columns
    item_df = pd.DataFrame()
    item_df['_id'] = ''

    for i in range(len(receipt_df['rewardsReceiptItemList'])):
        item_lst = receipt_df['rewardsReceiptItemList'].iloc[i]
        if type(item_lst) != float:
            for item in item_lst:
                item_df = pd.concat([item_df, pd.DataFrame.from_dict(item, orient='index').transpose()])
                item_df['_id'] = item_df['_id'].fillna(receipt_df.iloc[i]['_id'])


    return item_df
    

class Receipts:
    def __init__(self, fileName):
    
        # get the json dataset
        self.data = []

        # add raw data to data list
        for line in open(fileName, 'r'):
            self.data.append(json.loads(line))
        
    
class Brand:
    def __init__(self, fileName):
        """
        This class will be using 'with open' to read in dataset,
        store data into a Python dict, and then do data preprocessing.     
        """
        
        # get the json dataset
        self.data = []

        # add raw data to data list
        for line in open(fileName, 'r'):
            self.data.append(json.loads(line))
        
        self.brand_dict = {
            "_id": [],
            "barcode": [],
            "brandCode": [],
            "category": [],
            "categoryCode": [],
            "topBrand": [],
            "cpg": [],
            "cpg_ref":[],
            "name": []
        }
        
        # add value to the dictionary and do preprocessing at the same time
        # few missing column for each column 
        for row in self.data:
            for key, val in self.brand_dict.items():
                if key == 'cpg':
                    self.brand_dict['cpg'].append(row['cpg']['$id']['$oid'])
                    self.brand_dict['cpg_ref'].append(row['cpg']['$ref'])
                elif key == '_id':
                    self.brand_dict['_id'].append(row['_id']['$oid'])
                elif key not in row.keys():
                    if key != 'cpg_ref':             
                        self.brand_dict[key].append(None)
                else:
                    self.brand_dict[key].append(row[key])
        
    
    def create_df(self):                 
         
        df = pd.DataFrame(self.brand_dict)
        
        return df
      
                                  
        
# custom dataframe do the data quality check here 
class myCustom(pd.DataFrame):
    def __init__(self,  *args, **kwargs):
        super(myCustom, self).__init__( *args, **kwargs)
                
    @property
    def _constructor(self):
        return myCustom
    