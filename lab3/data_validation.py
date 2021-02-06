import pandas as pd
path = 'Oregon Hwy 26 Crash Data for 2019.xlsx'
df = pd.read_excel(path)
#data = pd.ExcelFile(path)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

CrashesDF = df[df['Record Type'] == 1]
VehiclesDF = df[df['Record Type'] == 2]
ParticipantsDF = df[df['Record Type'] == 3]

CrashesDF = CrashesDF.dropna(axis=1,how='all')
VehiclesDF = VehiclesDF.dropna(axis=1,how='all')
ParticipantsDF = ParticipantsDF.dropna(axis=1,how='all')

def crashEmptyTest(fieldName):
    for i in CrashesDF[fieldName]:
        if (i == ' '):
            print(1)
def vehicleEmptyTest(fieldName):
    for i in VehiclesDF[fieldName]:
        if (i == ' '):
            print(1)
def test2a():
    for i in CrashesDF['Serial #']:
        if (i < 1 or i > 99999):
            print(1)
def test2b():
    for i in CrashesDF['Crash Hour']:
        if (i<0 or i>99):
            print(1)
def test3a():
    df['date'] = None
    for i in df['date']:
        if(i == None):
            df['date'] = df['Crash Month'].astype(str) +"/"+ df['Crash Day'].astype(str) +"/"+ df["Crash Year"].astype(str)

def test3b():
    df['Latitude'] = None
    for i in df['Latitude']:
        if(i == None):
            df['Latitude'] = df['Latitude Degrees'].astype(str) +"/"+ df['Latitude Minutes'].astype(str) +"/"+ df["Latitude Seconds"].astype(str)

def test6a():
    for i in ParticipantsDF.index:
        if(ParticipantsDF['Participant Display Seq#'][i] == ' ' and ParticipantsDF['Participant ID'][i] == ' '):
            print('Not pair')
def test6b():
    for i in CrashesDF.index:
        if (CrashesDF['Serial #'][i] == ' ' and ParticipantsDF['Crash ID'][i] == ' '):
            print('Not pair')

def test7a():
    countWinter = 0
    countSummer = 0
    for i in CrashesDF.index:
        if(CrashesDF['Crash Month'] == 12,2,1):
            countWinter += 1
        if(CrashesDF['Crash Month'] == 6,7,8):
            countSummer += 1
    if(countWinter<countSummer):
        print("Summer has more crashes")
    else:
        print("Winter has more crashes")

test7a()

#crashEmptyTest('Crash ID')
#vehicleEmptyTest('Vehicle ID')

#print(df[:100])

