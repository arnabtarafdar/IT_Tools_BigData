
# coding: utf-8

# In[8]:

import pandas as pd
import os
os.system("pip install python-forecastio")
os.system("pip install azure-storage")
from azure.storage.blob import BlockBlobService


# In[10]:

import forecastio


# In[118]:

block_blob_service.get_blob_to_path('arnabcluster', 'Project/Weather/Weather.csv', 'Temp_Weather.csv')


# In[ ]:

data=pd.DataFrame({"cities": ["Bourg-en-Bresse" , "Laon" , "Ajaccio" , "Bastia" , "Moulins" , 
            "Digne-les-Bains" , "Gap" , "Nice" , "Privas" , "Charleville-Mezieres" , "Foix" , 
            "Troyes" , "Carcassonne" , "Rodez" , "Marseille" , "Caen" , "Aurillac" , "Angouleme" , 
            "La Rochelle" , "Bourges" , "Tulle" , "Dijon" , "Saint-Brieuc" , "Gueret" , "Perigueux" , 
            "Besancon" , "Valence" , "evreux" , "Chartres" , "Quimper" , "Nimes" , "Toulouse" , "Auch" ,
            "Bordeaux" , "Montpellier" , "Rennes" , "Chateauroux" , "Tours" , "Grenoble" , 
            "Lons-le-Saunier" , "Mont-de-Marsan" , "Blois" , "Saint-etienne" , "Le Puy-en-Velay" ,
            "Nantes" , "Orleans" , "Cahors" , "Agen" , "Mende" , "Angers" , "Saint-Lo" ,
            "Chalons-en-Champagne" , "Chaumont" , "Laval" , "Nancy" , "Bar-le-Duc" , "Vannes" , "Metz" , 
            "Nevers" , "Lille" , "Beauvais" , "Alencon" , "Arras" , "Clermont-Ferrand" , "Pau" , "Tarbes" ,
            "Perpignan" , "Strasbourg" , "Colmar" , "Lyon" , "Vesoul" , "Macon" , "Le Mans" , "Chambery" ,
            "Annecy" , "Paris" , "Rouen" , "Melun" , "Versailles" , "Niort" , "Amiens" , "Albi" , "Montauban" ,
            "Toulon" , "Avignon" , "La Roche-sur-Yon" , "Poitiers" , "Limoges" , "epinal" , "Auxerre" , 
            "Belfort" , "evry" , "Nanterre" , "Bobigny" , "Creteil" , "Cergy" , "Basse-Terre" ,
            "Fort-de-France" , "Cayenne" , "Saint-Denis" , "Saint-Pierre" , "Mamoudzou"],
 "latitude": [46.2051192 , 49.564665 , 41.9263991 , 42.7065505 , 46.566142 , 44.0918144 , 44.5611978 , 43.7009358 ,
              44.7352708 , 49.7599365 , 42.9660039 , 48.2971626 , 43.2130358 , 44.3510012 , 43.2961743 , 49.1828008 ,
              44.9285441 , 45.6496755 , 46.1591126 , 47.0805693 , 45.2678158 , 47.3215806 , 48.5141594 , 46.1686865 ,
              45.1909365 , 47.237953 , 44.9332277 , 49.0183639 , 48.4470039 , 47.9960325 , 43.8374249 , 43.6044622 ,
              43.6464483 , 44.841225 , 43.6112422 , 48.1113387 , 46.8047 , 47.3900474 , 45.182478 , 46.6739021 , 
              43.8911318 , 47.5876861 , 45.4401467 , 45.0459739 , 47.2186371 , 45.4810426 , 44.4495 , 44.2015624 , 
              44.5180226 , 47.4739884 , 49.1157004 , 48.9566218 , 45.119542 , 48.0710377 , 48.6937223 , 48.7736785 , 
              47.6586772 , 49.1196964 , 46.9945997 , 50.6305089 , 49.4409001 , 48.4312059 , 50.291048 , 45.7774551 , 
              43.2957547 , 43.232858 , 42.6953868 , 48.584614 , 48.0777517 , 45.7578137 , 47.6236088 , 32.8406946 , 
              48.0077781 , 45.5662672 , 45.8977758 , 48.8564915 , 49.4404591 , 48.539927 , 48.8035403 , 46.3239455 , 
              49.8941708 , 43.9277552 , 44.0175835 , 43.1257311 , 43.9493143 , 46.6705431 , 46.5802596 , 
              45.8354243 , 48.1745644 , 47.7954001 , 47.6379599 , 48.6311001 , 48.8924273 , 48.906387 , 
              48.7830727 , 49.0527528 , 16.0000778 , 14.6037193 , 4.9371143 , 48.935773 , -21.3412979 , -12.7805855],
"longitude": [5.2250324 , 3.620686 , 8.7376029 , 9.452542 , 3.333179 , 6.2351431 , 6.0820018 , 7.2683912 ,
              4.5986733 , 4.7186932 , 1.6096025 , 4.0746257 , 2.3491069 , 2.5733006 , 5.3699525 , -0.3690814 ,
              2.4433101 , 0.1568658 , -1.1520433 , 2.398932 , 1.7706904 , 5.0414701 , -2.7602706 , 1.8713349 ,
              0.7184407 , 6.0243246 , 4.8920811 , 1.1375865 , 1.4866387 , -4.1024781 , 4.3600687 , 1.4442469 ,
              0.5847904 , -0.5800363 , 3.8767337 , -1.6800197 , 1.6957099 , 0.6889268 , 5.7210773 , 5.5586167 ,
              -0.5009719 , 1.3337639 , 4.3873058 , 3.8855537 , -1.5541361 , -75.5099599 , 1.4364999 , 0.6176324 ,
              3.4991057 , -0.5515587 , -1.0906636 , 4.3628851 , 6.985306 , -0.7723498 , 6.1834097 , 5.1621546 ,
              -2.7599078 , 6.1763552 , 3.1591285 , 3.0706414 , 2.0866699 , 0.0911374 , 2.7772211 , 3.0819427 ,
              -0.3685667 , 0.0781021 , 2.8844713 , 7.7507127 , 7.3579641 , 4.8320114 , 6.1566367 , -83.6324021 ,
              0.1995339 , 5.9203636 , 6.1333989 , 2.3521562 , 1.0939658 , 2.6608169 , 2.1266886 , -0.4645211 ,
              2.2956951 , 2.147899 , 1.3549991 , 5.9304919 , 4.8060329 , -1.4269697 , 0.340196 , 1.2644847 , 
              6.4506416 , 3.58452 , 6.8628942 , 2.438 , 2.2071267 , 2.4452231 , 2.4518371 , 2.0388736 ,
              -61.7333372 , -61.0767676 , -52.3258306 , 2.3580232 , 55.4776174 , 45.2279908]
})


# In[119]:

if os.path.isfile('Temp_Weather.csv') == True:
    Weather = pd.read_csv('Temp_Weather.csv')
else:
    Weather = pd.DataFrame(columns=['Cities','Day','Min_Temperature','Max_Temperature','Precipitation_prob','Humidity','Wind_Speed','Summary'])


# In[13]:

temp = pd.DataFrame(columns=['Cities','Day','Min_Temperature','Max_Temperature','Precipitation_prob','Humidity','Wind_Speed','Summary'])

for i in data.itertuples():
    api_key = "56395e3fd1f33296c424c2c6e3ee008a"
    lat = i.latitude
    lng = i.longitude
    forecast = forecastio.load_forecast(api_key, lat, lng)
    bydaily = forecast.daily()
    day=bydaily.data[1]
    temp = temp.append(pd.Series([i.cities,day.time,day.temperatureMin,day.temperatureMax,day.precipProbability,day.humidity,day.windSpeed,day.summary],index=['Cities','Day','Min_Temperature','Max_Temperature','Precipitation_prob','Humidity','Wind_Speed','Summary']),ignore_index=True)


# In[133]:

Weather=Weather.append(temp,ignore_index=True)

# In[103]:

block_blob_service = BlockBlobService(account_name='storagefrd', account_key='YbZnDv9DZ6TmdPfzWW8sBeMiiro6qFyWtPZk/3ohQessmnB5BIOmhRGd7lOYUsgWLYNz1PmweBX+JKQxxL7lgA==')

# In[134]:

Weather.to_csv('Temp_Weather.csv')


# In[135]:

from azure.storage.blob import ContentSettings
block_blob_service.create_blob_from_path(
    'arnabcluster',
    'Project/Weather/Weather.csv',
    'Temp_Weather.csv',
    content_settings=ContentSettings(content_type='text/csv')
            )

