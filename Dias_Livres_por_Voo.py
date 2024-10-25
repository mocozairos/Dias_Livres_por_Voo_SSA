import streamlit as st
import mysql.connector
import decimal
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

st.set_page_config(layout='wide')

st.title('Dias Livres por Voo - Salvador')

st.divider()

def gerar_df_phoenix(vw_name, base_luck):
    # Parametros de Login AWS
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

if not 'df_router' in st.session_state:

    st.session_state.df_router = gerar_df_phoenix('vw_router', 'test_phoenix_salvador')

    st.session_state.df_router['Reserva Mae'] = st.session_state.df_router['Reserva'].str[:10]

row1 = st.columns(2)

row2 = st.columns(1)

with row1[0]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        st.session_state.df_router = gerar_df_phoenix('vw_router', 'test_phoenix_salvador')

        st.session_state.df_router['Reserva Mae'] = st.session_state.df_router['Reserva'].str[:10]

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_final')

df_in = st.session_state.df_router[(st.session_state.df_router['Data Execucao'] >= data_inicial) & 
                                    (st.session_state.df_router['Data Execucao'] <= data_final) & 
                                    (st.session_state.df_router['Tipo de Servico']=='IN') & (st.session_state.df_router['Status do Servico']!='CANCELADO')]\
                                        .reset_index(drop=True)

if len(df_in)>0:

    with row1[0]:

        lista_servicos = ['Todos']

        lista_servicos.extend(df_in['Servico'].unique().tolist())

        servico = container_datas.selectbox('Serviço', lista_servicos)

    if servico and servico!='Todos':

        df_in = df_in[df_in['Servico']==servico].reset_index(drop=True)

lista_reservas_in = df_in['Reserva Mae'].unique().tolist()

df_out = st.session_state.df_router[(st.session_state.df_router['Tipo de Servico']=='OUT') & (st.session_state.df_router['Status do Servico']!='CANCELADO') & 
                                    (st.session_state.df_router['Reserva Mae'].isin(lista_reservas_in))].reset_index(drop=True)

df_in_out = pd.merge(df_in[['Reserva Mae', 'Servico', 'Voo', 'Data Execucao']], df_out[['Reserva Mae', 'Data Execucao']], on='Reserva Mae', how='left')

df_in_out = df_in_out.rename(columns={'Data Execucao_x': 'Data IN', 'Data Execucao_y': 'Data OUT', 'Voo': 'Voo IN'})

df_in_out = df_in_out[~pd.isna(df_in_out['Data OUT'])].reset_index(drop=True)

df_tour_transfer = st.session_state.df_router[((st.session_state.df_router['Tipo de Servico']=='TOUR') | 
                                               (st.session_state.df_router['Tipo de Servico']=='TRANSFER')) & 
                                              (st.session_state.df_router['Status do Servico']!='CANCELADO') & 
                                              (st.session_state.df_router['Reserva Mae'].isin(lista_reservas_in))].reset_index(drop=True)

df_tour_transfer_group = df_tour_transfer.groupby(['Data Execucao', 'Reserva Mae'])['Servico'].count().reset_index()

df_tour_transfer_group = df_tour_transfer_group.groupby(['Reserva Mae'])['Servico'].count().reset_index()

df_tour_transfer_group = df_tour_transfer_group.rename(columns={'Servico': 'Qtd. Servicos'})

df_in_out = pd.merge(df_in_out, df_tour_transfer_group, on='Reserva Mae', how='left')

df_in_out['Data IN'] = pd.to_datetime(df_in_out['Data IN'])
df_in_out['Data OUT'] = pd.to_datetime(df_in_out['Data OUT'])

df_in_out['Dias Estadia'] = (df_in_out['Data OUT'] - df_in_out['Data IN']).dt.days

df_in_out['Dias Estadia'] = df_in_out['Dias Estadia'].astype(int)

df_in_out['Dias Estadia'] = df_in_out['Dias Estadia']-1

df_in_out['Qtd. Servicos'] = df_in_out['Qtd. Servicos'].fillna(0)

df_in_out['Dias Livres'] = df_in_out['Dias Estadia']-df_in_out['Qtd. Servicos']

df_final = df_in_out.groupby('Voo IN')['Dias Livres'].sum().reset_index()

df_final = df_final.sort_values(by=['Dias Livres'], ascending=False).reset_index(drop=True)

gb = GridOptionsBuilder.from_dataframe(df_final)
gb.configure_selection('multiple', use_checkbox=True)
gb.configure_grid_options(domLayout='autoHeight')
gb.configure_grid_options(domLayout='autoWidth')
gridOptions = gb.build()

with row1[1]:

    grid_response = AgGrid(df_final, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

selected_rows = grid_response['selected_rows']

if selected_rows is not None and len(selected_rows)>0:

    df_ref = df_in_out[df_in_out['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())].reset_index(drop=True)

    df_ref['Data IN'] = pd.to_datetime(df_ref['Data IN']).dt.date

    df_ref['Data OUT'] = pd.to_datetime(df_ref['Data OUT']).dt.date

    if servico!='Todos':

        with row2[0]:

            container_dataframe = st.container()

            container_dataframe.dataframe(df_ref[['Reserva Mae', 'Voo IN', 'Data IN', 'Data OUT', 'Qtd. Servicos', 'Dias Estadia', 'Dias Livres']]\
                                            .sort_values(by='Voo IN'), hide_index=True, use_container_width=True)
            
    else:

        df_ref = df_in_out[df_in_out['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())].groupby('Servico')['Dias Livres'].sum().reset_index()   

        gb = GridOptionsBuilder.from_dataframe(df_ref)
        gb.configure_selection('multiple', use_checkbox=True)
        gb.configure_grid_options(domLayout='autoWidth')
        gridOptions = gb.build()

        with row1[1]:

            grid_response = AgGrid(df_ref, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

        selected_rows_2 = grid_response['selected_rows']

        if selected_rows_2 is not None and len(selected_rows_2)>0:

            df_ref_2 = df_ref = df_in_out[(df_in_out['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())) & 
                                          (df_in_out['Servico'].isin(selected_rows_2['Servico'].unique().tolist()))].reset_index(drop=True)
            
            df_ref_2['Data IN'] = pd.to_datetime(df_ref_2['Data IN']).dt.date

            df_ref_2['Data OUT'] = pd.to_datetime(df_ref_2['Data OUT']).dt.date
            
            with row1[0]:

                container_dataframe = st.container()

                container_dataframe.dataframe(df_ref_2[['Reserva Mae', 'Servico', 'Voo IN', 'Data IN', 'Data OUT', 'Qtd. Servicos', 'Dias Estadia', 'Dias Livres']]\
                                                .sort_values(by='Voo IN'), hide_index=True, use_container_width=True)
