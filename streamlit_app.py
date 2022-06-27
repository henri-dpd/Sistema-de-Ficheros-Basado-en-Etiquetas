import sys
import streamlit as st
from client import client
    
def page1 (my_client):

    st.title('Sistema de ficheros')
    col1, col2 = st.columns(2)

    address = col1.text_input('Ingrese direccion ip')

    conect = col2.button("Existe")

    if conect and not address:
        st.subheader('Desconectado')
        st.error('Desconectado')
        st.warning('Ingrese direccion ip')
    elif conect and address:
        conectado = my_client.send_info(address, "are_you_alive",{})
        if conectado == "ERROR":
            st.subheader('Desconectado')
            st.error('Desconectado')
            st.warning('Ingrese direccion ip')
        else: 
            st.subheader('Conectado')
            st.success('Conectado')

    col1, col2 = st.columns(2)

    tag = col1.text_input('Ingrese etiqueta')

    search = col2.button('Buscar')

    if search and not tag:
        st.error('Verifica si estas conectado o introduce una etiqueta')
    elif search:
        print("hola")
        tags_dict = my_client.send_info(address, "get_tag", {'tag': tag})
        st.header('Ficheros:' + str(len(tags_dict["tags_object_id"])))
        print(tags_dict)
        if tags_dict:
            for i in tags_dict["tags_object_id"].values():
                c1, c2 = st.columns(2)
                c1.write(i)
                c2.button('Descargar',i)
                if c2 :
                    pass


def page2 (my_client):
    st.title('Subir ficheros')
    files = st.file_uploader("File",accept_multiple_files=True)
    
    if files is None:
        st.write("Carga ficheros")
    else :
        st.header('Ficheros cargados')
        list_files = [file.name for file in files]
        for i in list_files:
            c1, c2, c3 = st.columns(3)
            c1.write(i)
            c2.text_input('Etiquetas de ' + i)
            c3.button('Subir', i)
            if c3 and not c2:
                st.error("No has puesto etiquetas")
            elif c3 and c2:
                pass


page_names = {
    "Page_1" : page1,
    "Page_2" : page2
}

if __name__ == "__main__":
    my_client = client(sys.argv[1]) 
    select_page = st.sidebar.selectbox('Seleccionar pagina', page_names.keys())
    page_names[select_page](my_client) 
    