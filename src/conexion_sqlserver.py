import pyodbc
import pandas as pd
from datetime import datetime

class ImportDataBD():

    def run(self):
        try:
            self.__connection = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-PJLPNQF;DATABASE=AuxAlmacen;Trusted_Connection=yes;')
            print("===========================================================")
            print("Conexión exitosa.")
            self.__cursor = self.__connection.cursor()
            self.__cursor.execute("SELECT @@version;")
            row = self.__cursor.fetchone()
            print("Versión del servidor de SQL Server: {}".format(row))
            print("===========================================================")


            self.__dataAlmacenes()
            self.__dataCategorias()
            self.__dataUnidadMedida()
            self.__dataArticulos()
            self.__dataControlStock()
            self.__dataTipoTransaccion()
            self.__dataProveedores()
            self.__dataIngresoSalida()            
            self.__detIngresoSalida()
            self.__GrupoAcceso()
            self.__grupoClave()
            self.__Pedido()
            self.__detPedido()
            self.__dataPr_Pg_Py()
            
        except Exception as ex:
            print("**********************************************************")
            print("Error durante la conexión: {}".format(ex))
            print("**********************************************************")
        finally:
            self.__connection.close()  # Se cerró la conexión a la BD.
            print("===========================================================")
            print("La conexión ha finalizado.")
            print("===========================================================")

    def __llenarCeros(self,x,maxLen,c):
        maxLen += c
        return str(str(x).rjust(maxLen, '0'))

    def __dataAlmacenes(self):

        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\Almacen.txt", sep = ",",dtype='str', encoding='UTF-16')
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            #---------------------------------------------

            for i in range(df.shape[0]):
                self.__cursor.execute("insert into AL_ALMACEN(cod_almacen,nom_almacen,dir_almacen,tlf_almacen,Obs) values (?, ?, ?, ?, ?)",
                    df['cod_almacen'][i],
                    df['nom_almacen'][i],
                    df['dir_almacen'][i],
                    df['tlf_almacen'][i],
                    df['Obs'][i])
            #commit the transaction
            self.__connection.commit()

            print("Insercion de Almacenes")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataArticulos(self):

        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\Articulos.txt", sep = "|",dtype='str', encoding='UTF-16', on_bad_lines='skip')
            values = {"Imagen": '', "des_articulo": '', "Obs": '', "ubicacion": '',"precio_promedio_ref":0,"precio_ultimo_ref":0}
            df.fillna(value=values,inplace=True)

            lisint = ['cod_articulo']
            lisStr = ['cod_und_medida','Cod_categoria','nom_articulo','des_articulo','ubicacion','Imagen','Obs'	]
            lisbool = ['perecible','estado','visible']
            lisFloat = ['precio_promedio_ref','precio_ultimo_ref']

            

            df[lisStr] = df[lisStr].astype('string')
            df[lisbool] = df[lisbool].astype('bool')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisint] = df[lisint].astype('int64')
            
            lisbool = df.select_dtypes(include=['bool']).columns.values
            df[lisbool] = df[lisbool].astype(int)
            #---------------------------------------------
            self.__cursor.execute("SET IDENTITY_INSERT AL_ARTICULO ON")
            for i in range(df.shape[0]):
                a = int(df['cod_articulo'][i])
                b1 = bool(df['perecible'][i])
                b2 = bool(df['estado'][i])
                b3 = bool(df['visible'][i])
                self.__cursor.execute("insert into AL_ARTICULO(cod_articulo,cod_und_medida,Cod_categoria,nom_articulo,des_articulo,perecible,ubicacion,estado,Imagen,precio_promedio_ref,precio_ultimo_ref,Obs,visible)values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    a
                    ,df['cod_und_medida'][i]
                    ,df['Cod_categoria'][i]
                    ,df['nom_articulo'][i]
                    ,df['des_articulo'][i]
                    ,b1
                    ,df['ubicacion'][i]
                    ,b2
                    ,None
                    ,df['precio_promedio_ref'][i]
                    ,df['precio_ultimo_ref'][i]
                    ,df['Obs'][i]
                    ,b3)
            #commit the transaction
            
            
            self.__cursor.execute("SET IDENTITY_INSERT AL_ARTICULO OFF")
            self.__connection.commit()
            print("Insercion de Articulos")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataCategorias(self):

        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\Categoria.txt", sep = ",",dtype='str', encoding='UTF-16')
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')

            #---------------------------------------------

            for i in range(df.shape[0]):
                self.__cursor.execute("insert into AL_CATEGORIA(Cod_categoria,nom_categoria,des_categoria,Obs) values (?, ?, ?, ?)",
                    df['Cod_categoria'][i],
                    df['nom_categoria'][i],
                    df['des_categoria'][i],
                    df['Obs'][i])
            #commit the transaction
            self.__connection.commit()
            print("Insercion de Categoria")
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataUnidadMedida(self):

        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\\DataBd\\UnidadMedida.txt", sep = ",",dtype='str', encoding='UTF-16')
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')

            #---------------------------------------------

            for i in range(df.shape[0]):
                self.__cursor.execute("insert into AL_UND_MEDIDA(cod_und_medida,des_und_medida,Obs) values (?, ?, ?)",
                    df['cod_und_medida'][i],
                    df['des_und_medida'][i],
                    df['Obs'][i])
            #commit the transaction
            self.__connection.commit()
            print("Insercion de UnidadMedida")
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataControlStock(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\controlStock.txt", sep = ",",dtype='str', encoding='UTF-16')
            
            lisint = ['cod_articulo']
            lisStr = ['cod_almacen']
            lisFloat = ['cant_fisica','cant_real','cant_minima_reposicion']

            df[lisStr] = df[lisStr].astype('string')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisint] = df[lisint]

            #---------------------------------------------

            for i in range(df.shape[0]):
                a = int(df['cod_articulo'][i])
                self.__cursor.execute("insert into AL_CONTROL_STOCK(cod_almacen,cod_articulo,cant_fisica,cant_real,cant_minima_reposicion,Obs)values (?, ?, ?, ?, ?, ?)"
                    ,df['cod_almacen'][i]
                    ,a
                    ,df['cant_fisica'][i]
                    ,df['cant_real'][i]
                    ,df['cant_minima_reposicion'][i]
                    ,None)
            #commit the transaction
            self.__connection.commit()
            print("Insercion de ControlStock")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataIngresoSalida(self):
        
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\IngresoSalida2.txt", sep = "|",dtype='str', encoding='UTF-16')
            df.dropna(subset=['fecha_proceso'],inplace = True)
            df.fillna('',inplace=True)
            lisint = ['id_transaccion','cod_clave']
            lisStr = ['fecha_transaccion','fecha_proceso','cod_tipo_transaccion','num_guia','cod_proveedor','cod_almacen','Obs']
            lisBool = ['ingreso','procesado','pendiente']
            #listFecha = ['fecha_transaccion','fecha_proceso']
            lisFloat = []
            
            df['fecha_proceso'] = df['fecha_proceso'].astype('datetime64')
            df[lisStr] = df[lisStr].astype('string')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisBool] = df[lisBool].astype('bool')
            
            lisbool = df.select_dtypes(include=['bool']).columns.values
            df[lisbool] = df[lisbool].astype(int)
            
            df = df.reset_index()
            #---------------------------------------------
            self.__cursor.execute("SET IDENTITY_INSERT AL_INGRESO_SALIDA ON")
            for index, row in df.iterrows():
            # for i in range(df.shape[0]):
                a = int(row['id_transaccion'])
                b = int(row['cod_clave'])
                f1 = datetime.strptime(row['fecha_transaccion'], '%Y-%m-%d %H:%M:%S')
                f2 = datetime.strptime(row['fecha_proceso'], '%Y-%m-%d %H:%M:%S.%f')
                b1 = bool(row['ingreso'])
                b2 = bool(row['procesado'])
                b3 = bool(row['pendiente'])
                self.__cursor.execute("insert into AL_INGRESO_SALIDA(id_transaccion,cod_clave,ingreso,cod_tipo_transaccion,num_guia,fecha_transaccion,cod_proveedor,cod_almacen,Obs,procesado,pendiente,fecha_proceso)values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    ,a
                    ,b
                    ,b1
                    ,row['cod_tipo_transaccion']
                    ,row['num_guia']
                    ,f1
                    ,row['cod_proveedor']
                    ,row['cod_almacen']
                    ,row['Obs']
                    ,b2
                    ,b3
                    ,f2)
            #commit the transaction
            self.__connection.commit()
            self.__cursor.execute("SET IDENTITY_INSERT AL_INGRESO_SALIDA OFF")
            print("Insercion de IngresoSalida")
               
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __dataTipoTransaccion(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\TipoTransaccion.txt", sep = ",",dtype='str', encoding='UTF-16')
            
            lisbool = ['ingreso']
            lisStr = ['cod_tipo_transaccion','descripción']
            
            df[lisbool] = df[lisbool].astype('bool')
            df[lisStr] = df[lisStr].astype('string')
            
            lisbool = df.select_dtypes(include=['bool']).columns.values
            df[lisbool] = df[lisbool].astype(int)
            #---------------------------------------------
            
            for i in range(df.shape[0]):
                a = bool(df['ingreso'][i])
                self.__cursor.execute("insert into AL_TIPO_TRANSACCION(cod_tipo_transaccion,descripción,ingreso)values (?, ?, ?)"
                    ,df['cod_tipo_transaccion'][i]
                    ,df['descripción'][i]
                    ,a)
            #commit the transaction
            self.__connection.commit()
            print("Insercion de TipoTransacción")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")
            
            
    def __dataProveedores(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\proveedor2.txt", sep = "|",dtype='str', encoding='UTF-16',on_bad_lines='skip')
            values = {"cod_categoria": '006'}
            df.fillna(value=values,inplace=True)
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            aux = df[(df['cod_categoria'] == "alvanasociados@terra.com.pe") | 
                    (df['cod_categoria'] == "siml@terra.com.pe") |
                    (df['cod_categoria'] == "SERVICIOS POSTALES DEL PERU S.A.") |
                    (df['cod_categoria'] == "custsvc@iwpnews.com") | 
                    (df['cod_categoria'] == "service@iwpnews.com") |
                    (df['cod_categoria'] == "AV. RICARDO PALMA 950 DPTO. 704 URB. MIRAFLORES MIRAFLORES")]

            df.drop(aux.index, inplace = True)
            #---------------------------------------------
            
            for i in range(df.shape[0]):
                aux = df.iloc[[i]]
                aux['cod_proveedor'].values[0]
                
                self.__cursor.execute("insert into AL_PROVEEDOR(cod_proveedor,razon_social,Cod_categoria,direccion,ciudad,pais,telefono,fax,web,Obs,contacto,beneficiario,activo,RUC,codigoPostal,Posicion,titulo,saludo) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    ,aux['cod_proveedor'].values[0]
                    ,aux['razon_social'].values[0]
                    ,aux['cod_categoria'].values[0]
                    ,aux['direccion'].values[0]
                    ,aux['ciudad'].values[0]
                    ,aux['pais'].values[0]
                    ,aux['telefono'].values[0]
                    ,aux['fax'].values[0]
                    ,aux['web'].values[0]
                    ,aux['Obs'].values[0]
                    ,aux['contacto'].values[0]
                    ,aux['beneficiario'].values[0]
                    ,aux['activo'].values[0]
                    ,aux['RUC'].values[0]
                    ,aux['codigoPostal'].values[0]
                    ,aux['Posicion'].values[0]
                    ,aux['titulo'].values[0]
                    ,aux['saludo'].values[0])
            #commit the transaction
            self.__connection.commit()
            print("Insercion de Proveedores")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")
            
    def __detIngresoSalida(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\DetIngresoSalida.txt", sep = ",",dtype='str', encoding='UTF-16')
            values = {"Imagen": '', "des_articulo": '', "Obs": '', "ubicacion": '',"precio_promedio_ref":0,"precio_ultimo_ref":0}
            df.fillna(value=values,inplace=True)
            
            df[df.columns.values] = df[df.columns.values].astype('string')
            #---------------------------------------------

            for i in range(df.shape[0]):
                self.__cursor.execute("insert into AL_ALMACEN(cod_almacen,nom_almacen,dir_almacen,tlf_almacen,Obs) values (?, ?, ?, ?, ?)",
                    df['cod_almacen'][i],
                    df['nom_almacen'][i],
                    df['dir_almacen'][i],
                    df['tlf_almacen'][i],
                    df['Obs'][i])
            #commit the transaction
            self.__connection.commit()
            print("Insercion de IngresoSalida")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")
            
    def __detIngresoSalida(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\DetingresSalida2.txt", sep = "|",dtype='str', on_bad_lines='skip',encoding='UTF-16')
            values = {"costo_unitario": '0'}
            df.fillna(value=values,inplace=True)
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            lisinit = ['id_transaccion','cod_articulo']
            lisFloat = ['cant_articulo','costo_unitario']
            lisbool = []
            lisStr = ['Obs']
            
            df[lisinit] = df[lisinit].astype('int64')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisStr] = df[lisStr].astype('string')
            
            #---------------------------------------------

            for index, row in df.iterrows():
                self.__cursor.execute("insert into AL_DET_INGRESO_SALIDA(id_transaccion,cod_articulo,cant_articulo,costo_unitario,Obs,fecha_vencimiento)values (?, ?, ?, ?, ?, ?)"
                    ,row['id_transaccion']
                    ,row['cod_articulo']
                    ,row['cant_articulo']
                    ,row['costo_unitario']
                    ,row['Obs']
                    ,None)
            #commit the transaction
            self.__connection.commit()
            print("Insercion de DetIngresoSalida")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")     

    def __Pedido(self):
        
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\Pedidos2.txt", sep = "|",dtype='str', on_bad_lines='skip',encoding='UTF-16')
            df.dropna(subset=['atendido','piso_destino'],inplace = True)
            values = {"fecha_despacho": 'None',"fecha_entrega": 'None'}
            df.fillna(value=values,inplace=True)
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            lisinit = ['id_pedido','cod_clave']
            lisFloat = []
            lisbool = ['autorizado','urgente','recepcionado','enviado','atendido']
            lisFecha = ['fecha_pedido','fecha_entrega','fecha_despacho']
            lisStr = ['cod_almacen','piso_destino','proc_destino','prog_destino','proy_destino','motivo_solicitud','Obs','pedido_por']
            
            df[lisinit] = df[lisinit].astype('int64')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisStr+lisFecha] = df[lisStr+lisFecha].astype('string')
            df[lisbool] = df[lisbool].astype('bool')
            
            #---------------------------------------------
            self.__cursor.execute("SET IDENTITY_INSERT AL_PEDIDO ON")
            for index, row in df.iterrows():
                if (row['fecha_pedido'] !='None'): f1= datetime.strptime(row['fecha_pedido'], '%Y-%m-%d %H:%M:%S') 
                else: f1= None
                if (row['fecha_entrega'] !='None'): f2= datetime.strptime(row['fecha_entrega'], '%Y-%m-%d %H:%M:%S') 
                else: f2= None
                if (row['fecha_despacho'] !='None'): f3= datetime.strptime(row['fecha_despacho'], '%Y-%m-%d %H:%M:%S') 
                else: f3= None
                self.__cursor.execute("insert into AL_PEDIDO(id_pedido,cod_clave,cod_almacen,fecha_pedido,fecha_entrega,fecha_despacho,piso_destino,proc_destino,prog_destino,proy_destino,motivo_solicitud,autorizado,urgente,recepcionado,enviado,Obs,atendido,pedido_por)values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    ,row['id_pedido']
                    ,row['cod_clave']
                    ,row['cod_almacen']
                    ,f1
                    ,f2
                    ,f3
                    ,row['piso_destino']
                    ,row['proc_destino']
                    ,row['prog_destino']
                    ,row['proy_destino']
                    ,row['motivo_solicitud']
                    ,row['autorizado']
                    ,row['urgente']
                    ,row['recepcionado']
                    ,row['enviado']
                    ,row['Obs']
                    ,row['atendido']
                    ,None)
            #commit the transaction
            self.__connection.commit()
            self.__cursor.execute("SET IDENTITY_INSERT AL_PEDIDO OFF")
            print("Insercion de Pedido")
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")     
                     
                                 
    def __detPedido(self):
        
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\DetPedido2.txt", sep = "|",dtype='str', on_bad_lines='skip',encoding='UTF-16')
            df.dropna(subset=['cod_articulo','cant_pedida','pedido_para_compra','autoriza_compra'],inplace = True)
            values = {"cant_aceptada": '0',"cant_entregada": '0',"cant_por_entregar": '0',"costo_cant_entrega": '0'}
            df.fillna(value=values,inplace=True)
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            lisinit = ['id_pedido','cod_articulo']
            lisFloat = ['cant_pedida','cant_aceptada','cant_entregada','cant_por_entregar','costo_cant_entrega']
            lisbool = ['pedido_para_compra','autoriza_compra']
            lisStr = ['Obs']
            
            df[lisinit] = df[lisinit].astype('int64')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisStr] = df[lisStr].astype('string')
            df[lisbool] = df[lisbool].astype('bool')
            
            #---------------------------------------------
            ped = pd.read_csv("src\DataBd\Pedidos2.txt", sep = "|",dtype='str', on_bad_lines='skip',encoding='UTF-16')
            ped.dropna(subset=['atendido','piso_destino'],inplace = True)
            ped['id_pedido'] = df['id_pedido'].astype('int64')
            ped.reset_index()
            aux = ped['id_pedido'].to_list()        
            for index, row in df.iterrows():
                if row['id_pedido'] in aux:
                    self.__cursor.execute("insert into AL_DET_PEDIDO(id_pedido,cod_articulo,cant_pedida,cant_aceptada,cant_entregada,cant_por_entregar,costo_cant_entrega,pedido_para_compra,autoriza_compra,Obs)values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        ,row['id_pedido']
                        ,row['cod_articulo']
                        ,row['cant_pedida']
                        ,row['cant_aceptada']
                        ,row['cant_entregada']
                        ,row['cant_por_entregar']
                        ,row['costo_cant_entrega']
                        ,row['pedido_para_compra']
                        ,row['autoriza_compra']
                        ,row['Obs'])
            #commit the transaction
               
            self.__connection.commit()
            print("Insercion de DetPedido")
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")     
                     
    def __GrupoAcceso(self):
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\GrupoAcceso.txt", sep = ",",dtype='str', encoding='UTF-16')
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            #---------------------------------------------

            for i in range(df.shape[0]):
                self.__cursor.execute("insert into AL_GRUPO_ACCESO(Cod_grupo,Descripcion) values (?, ?)"
                    ,df['Cod_grupo'][i]
                    ,df['Descripcion'][i])
            #commit the transaction
            self.__connection.commit()
            print("Insercion de GrupoAcceso")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")

    def __grupoClave(self):
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\GrupoClave2.txt", sep = "|",dtype='str', encoding='UTF-16')
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            lisinit = ['cod_clave']
            
            df[lisinit] = df[lisinit].astype('int64')
            
            #---------------------------------------------
            self.__cursor.execute("SET IDENTITY_INSERT AL_GRUPO_CLAVE ON")
            for index, row in df.iterrows():
                self.__cursor.execute("insert into AL_GRUPO_CLAVE(cod_clave,Cod_funcionario,Cod_grupo,clave,apoya_a,ppp) values (?, ?, ?, ?, ?, ?)"
                    ,row['cod_clave']
                    ,row['Cod_funcionario']
                    ,row['Cod_grupo']
                    ,row['clave']
                    ,row['apoya_a']
                    ,row['ppp'])
            #commit the transaction
            self.__cursor.execute("SET IDENTITY_INSERT AL_GRUPO_CLAVE OFF")
            self.__connection.commit()
            print("Insercion de GrupoClave")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")
            
    def __dataPr_Pg_Py(self):
    
        try:
            #TRATAMIENTO DE LA DATA CON PANDAS
            df = pd.read_csv("src\DataBd\PgPyPr.txt", sep = "|",dtype='str', encoding='UTF-16')
            df['fecha_modificacion'] = df['fecha_modificacion'].astype('datetime64')
            values = {"fecha_modificacion": 'None',"precedente": '-1', 'user_modificacion':''}
            df.fillna(value=values,inplace=True)
            df.fillna('',inplace=True)
            df[df.columns.values] = df[df.columns.values].astype('string')
            
            lisinit = ['precedente']
            lisFloat = []
            lisbool = []
            lisFecha = ['fecha_modificacion']
            lisStr = ['numero_ppp','cod_interno','Descripcion','observaciones','obj_general','user_modificacion','obj_general2']
            
            df[lisinit] = df[lisinit].astype('int64')
            df[lisFloat] = df[lisFloat].astype('float64')
            df[lisStr+lisFecha] = df[lisStr+lisFecha].astype('string')
            df[lisbool] = df[lisbool].astype('bool')
            
            #---------------------------------------------

            for index, row in df.iterrows():
            
                if (row['fecha_modificacion'] !='None'): 
                    if (row['fecha_modificacion'].find('.')!=-1):
                        f1= datetime.strptime(row['fecha_modificacion'], '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        f1= datetime.strptime(row['fecha_modificacion'], '%Y-%m-%d %H:%M:%S')
                else: f1= None
                if (row['precedente'] !='-1'): p1= row['precedente']
                else: p1= None
                
                self.__cursor.execute("insert into Pr_Pg_Py(numero_ppp,cod_interno,Descripcion,precedente,observaciones,obj_general,fecha_modificacion,user_modificacion,obj_general2)values (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    ,row['numero_ppp']
                    ,row['cod_interno']
                    ,row['Descripcion']
                    ,p1
                    ,None
                    ,None
                    ,f1
                    ,row['user_modificacion']
                    ,None)
            #commit the transaction
            self.__connection.commit()
            print("Insercion de Pr_Pg_Py")
            
        except Exception as ex:
            print("**********************************************************")
            print("Error al importar data almacenes {}".format(ex))
            print("**********************************************************")
                             
importador = ImportDataBD()
importador.run()