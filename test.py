# main.py
from conneppy.Conneppy import Conneppy
import pandas as pd

# Creazione dell'oggetto Database
conneppy = Conneppy(db_type='sqlserver', username='sa', password='password01!', host='WINDELL-186CUHK\SQLEXPRESS'
                    , port=3306, database='NAUS_PROD')

# Utilizzo di una sessione per eseguire query
session = conneppy.get_session()
try:
    # Qui puoi eseguire le tue query utilizzando `session`
  #  result = conneppy.table_info("AFFITTI")['fields']
    result = conneppy.select_join("AFFITTI")
    # for i in range(0,2):
    #     print(result[i])
    df_affitti = pd.DataFrame(result)
    print(df_affitti.head())
    pass
finally:
    conneppy.close(session)
