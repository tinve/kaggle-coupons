from __future__ import division

import pandas as pd
import graphlab as gl

start_time = pd.to_datetime('2011-06-27')
end_time = pd.to_datetime('2012-12-30')
time_length = int((end_time - start_time).days)



# ## user_list.csv

users = pd.read_csv('user_list.csv', index_col = None)
users.drop('PREF_NAME', axis = 1, inplace = True)
users['SEX_ID'] = users['SEX_ID'].map({'f':0, 'm': 1})

users['REG_DATE'] = pd.to_datetime(users['REG_DATE'])
users['WITHDRAW_DATE'] = pd.to_datetime(users['WITHDRAW_DATE'])
users['WITHDRAW_DATE'] =users['WITHDRAW_DATE'].fillna(end_time)

# REG_DATE to number of days since start time. Negative numbers to zero.
users['REG_DATE'] = users['REG_DATE'].map(lambda x : 0 if x < start_time else (x - start_time).days)
users['WITHDRAW_DATE'] = users['WITHDRAW_DATE'].map(lambda x : (x - start_time).days)

print 'user data processed'



# ## coupon_list_train_translated.csv
# ## coupon_list_test_translated.csv

train = pd.read_csv('coupon_list_train_translated.csv', index_col = None)
train['test'] = 0
test = pd.read_csv('coupon_list_test_translated.csv', index_col = None)
test['test'] = 1

joined = pd.concat([train, test])

# USABLE_DATE_SOMETHING: replace 2 and NaN with 1
for col in joined.iloc[:, 11:20]:
    joined[col] = joined[col].map({0:0, 1:1, 2:1})
    joined[col] = joined[col].fillna(1)

# VALIDFROM to number of days since start time
# Dirty solution? Assumes coupons with NaN action period are active all time
joined.drop('VALIDEND', axis = 1, inplace = True)
joined['VALIDPERIOD'] = joined['VALIDPERIOD'].fillna(time_length)
joined['VALIDPERIOD'] = joined['VALIDPERIOD'].map(int)

joined['VALIDFROM'] = pd.to_datetime(joined['VALIDFROM'])
joined['VALIDFROM'] = joined['VALIDFROM'].fillna(start_time)
joined['VALIDFROM'] = joined['VALIDFROM'].map(lambda x: (x - start_time).days)

# DISPFROM to number of days since start time
joined.drop('DISPEND', axis = 1, inplace = True)

joined['DISPFROM'] = pd.to_datetime(joined['DISPFROM'])
joined['DISPFROM'] = joined['DISPFROM'].map(lambda x: (x - start_time).days)

print 'coupon data processed'



# ## coupon_visit_train.csv

visit = pd.read_csv('coupon_visit_train.csv')
visit.drop(['PAGE_SERIAL', 'REFERRER_hash', 'SESSION_ID_hash', 'I_DATE'], axis = 1, inplace = True)

visit = visit.drop_duplicates()

index_id = visit.drop_duplicates().groupby(['USER_ID_hash', 'VIEW_COUPON_ID_hash']).sum().reset_index()
index_id['PURCHASE_FLG'] = index_id['PURCHASE_FLG'].map(lambda x: 1 if x > 0 else 0)
index_id.rename(columns={'VIEW_COUPON_ID_hash' : 'COUPON_ID_hash'}, inplace = True)

print 'visit data processed'



# ## coupon_detail_train.csv

detail = pd.read_csv('coupon_detail_train.csv')
detail.drop(['ITEM_COUNT', 'I_DATE', 'SMALL_AREA_NAME', 'PURCHASEID_hash'], axis = 1, inplace = True)
detail['PURCHASE_FLG'] = 1

print 'detail data processed'



index_id = pd.concat([index_id, detail]).drop_duplicates()

big = joined.merge(index_id)
big = big.merge(users)

print 'user and coupon data merged'



# # Create new training data

big_train = big[big['test'] == 0]

target = big_train['PURCHASE_FLG']
big_train.drop(['PURCHASE_FLG', 'test', 'COUPON_ID_hash', 'USER_ID_hash', 'small_area_name'], axis = 1, inplace = True)

print 'big_train dataframe created'

big_train = gl.SFrame(big_train)

print 'big_train data converted'

big_train.save('big_train.data', format = 'binary')

print 'big_train data saved'



# # Create new testing data

test = joined[joined['test'] == 1]
test.drop('small_area_name', axis = 1, inplace= True)
users['test'] = 1

big_test = pd.merge(test, users, on = 'test', how = 'outer').drop('test', axis = 1)

users.drop('test', axis = 1, inplace = True)
big_test = big_test[ [col for col in big_test if col != 'COUPON_ID_hash'] + ['COUPON_ID_hash'] ]

print 'big_test data created'

big_test = gl.SFrame(big_test)

print 'big_test data converted'

big_test.save('big_test.data', format = 'binary')

print 'big_test data saved'



