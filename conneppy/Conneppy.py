# database.py
from  datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, select, join, outerjoin,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import aliased
from sqlalchemy.engine.reflection import Inspector

class Conneppy:
    def __init__(self, db_type, username, password, host, port, database, **kwargs):
        self.db_type = db_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.kwargs = kwargs  # Salva i kwargs per usarli in connect
        self.engine = None
        self.SessionLocal = None
        self.metadata = MetaData()
        self.connect()

    def connect(self):
        # Mappa i tipi di database a stringhe di connessione
        db_urls = {
            'mysql': f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            'postgresql': f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            'sqlite': f"sqlite:///{self.database}",
            'sqlserver': f"mssql+pyodbc://{self.username}:{self.password}@{self.host}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        }
        if self.db_type not in db_urls:
            raise ValueError("Tipo di DB non supportato")

        # Crea l'engine di SQLAlchemy usando kwargs
        self.engine = create_engine(db_urls[self.db_type], **self.kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.metadata.reflect(bind=self.engine)

    def table_info(self, table_name, schema=None):
        inspector = Inspector.from_engine(self.engine)
        columns = inspector.get_columns(table_name, schema=schema)
        pk = inspector.get_pk_constraint(table_name, schema=schema)
        fks = inspector.get_foreign_keys(table_name, schema=schema)
        pk_columns = set(pk['constrained_columns']) if pk else set()
        fk_columns = {fk['constrained_columns'][0] for fk in fks} if fks else set()

        fields = []
        for column in columns:
            field_info = {
                'NAME': column['name'],
                'TYPE': str(column['type']),
                'IS_PK': column['name'] in pk_columns ,
                'IS_FK': column['name'] in fk_columns ,
                'DEFAULT': column['default'] if column['default'] is not None else ''
            }
            fields.append(field_info)

        return {
            'table': table_name,
            'fields': fields
        }
    
    def table_relationships(self, table_name, schema=None):
        inspector = Inspector.from_engine(self.engine)
        fks = inspector.get_foreign_keys(table_name, schema=schema)
        related_tables = [fk['referred_table'] for fk in fks]

        return {
            'NAME': table_name,
            'RELATIONSHIPS': related_tables
        }

    def select(self, table_name, fields=None, where_clause=None, schema=None):
        """
        Esegue una query SELECT su una tabella specificata con opzioni per i campi,
        clausole WHERE e schemi.
        """
        # Se non sono specificati campi, selezionare tutto
        if fields is None:
            fields = [text('*')]
        else:
            fields = [text(field) for field in fields]

        # Rifletti la tabella desiderata dal database
        table_info = f"{schema}.{table_name}" if schema else table_name
        print(table_info)
        table = self.metadata.tables.get(table_info)
        if table is None:  # Controlla se la tabella è stata trovata
            raise ValueError(f"Tabella {table_name} non trovata nel database.")

        # Costruisci la query di selezione
        query = select(*fields).select_from(table)

        # Aggiungi condizioni where se presenti
        if where_clause:
            for key, value in where_clause.items():
                query = query.where(table.c[key] == value)

        # Esegui la query utilizzando una sessione
        with self.get_session() as session:
            result = session.execute(query)
            # Restituisce i risultati come lista di dizionari
            results_list = []
            for row in result:
                # Converti la tupla in un dizionario
                row_dict = dict(zip(result.keys(), row))
                results_list.append(row_dict)
            return results_list

    
    def select_join(self, table_name, is_inner=False):
        inspector = Inspector.from_engine(self.engine)
        
        if table_name not in self.metadata.tables:
            raise ValueError(f"La tabella {table_name} non è stata trovata nel metadata.")
        
        main_table = self.metadata.tables[table_name]
        field_names = [f"{table_name}_{col}" for col in main_table.columns.keys()]
        
        stmt = select(*[main_table.c[col].label(f"{table_name}_{col}") for col in main_table.columns.keys()])
        aliased_tables = {}

        for fk in inspector.get_foreign_keys(table_name):
            fk_table_name = fk['referred_table']
            fk_table = self.metadata.tables.get(fk_table_name)
            if fk_table is None:
                continue
            
            if fk_table_name not in aliased_tables:
                aliased_tables[fk_table_name] = []
            new_alias = aliased(fk_table)
            aliased_tables[fk_table_name].append(new_alias)
            
            fk_field_names = [f"{fk_table_name}_{len(aliased_tables[fk_table_name])}_{col}" for col in fk_table.columns.keys()]
            field_names.extend(fk_field_names)
            
            fk_columns = [new_alias.c[col].label(fk_field_names[i]) for i, col in enumerate(fk_table.columns.keys())]
            stmt = stmt.add_columns(*fk_columns)

            pk_column = new_alias.c[fk['referred_columns'][0]]
            fk_column = main_table.c[fk['constrained_columns'][0]]

            if is_inner:
                stmt = stmt.join(new_alias, fk_column == pk_column)
            else:
                stmt = stmt.outerjoin(new_alias, fk_column == pk_column)

        with self.get_session() as session:
            result = session.execute(stmt)
            data = [field_names]
            for row in result:
                formatted_row = []
                for value in row:
                    if isinstance(value, datetime):
                        formatted_row.append(value.strftime('%Y-%m-%d'))
                    else:
                        formatted_row.append(value)
                data.append(formatted_row)
            return data



    def get_session(self):
        return self.SessionLocal()

    def close(self, session):
        session.close()

