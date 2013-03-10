from db import DProfiles 

""" main """
dp= DProfiles()
print "Trying insert"
dp.insert({'profile_alias':'test_alias',  'auth_data':'xxx', 'user_id':12345, 'flag': DProfiles.FLAG_NONE})
dp.insert({'profile_alias':'tom',  'auth_data':'xxx', 'user_id':123456, 'flag': DProfiles.FLAG_NONE})

dp.update('tom',{'auth_data':'foofoofoo'})

dp.delete('test_alias')
