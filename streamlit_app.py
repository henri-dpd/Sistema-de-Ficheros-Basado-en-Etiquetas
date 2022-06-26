import streamlit as st

def page1 ():

    st.title('Sistema de ficheros')
    col1, col2 = st.columns(2)

    address = col1.text_input('Ingrese direccion ip')

    conect = col2.button("Conectar")

    conection = False

    if conect and not address:
        st.subheader('Desconectado')
        st.error('Desconectado')
        st.warning('Ingrese direccion ip')
        conection = False
    elif conect and address:
        st.subheader('Conectado')
        st.success('Conectado')
        conection = True


    col1, col2 = st.columns(2)

    tag = col1.text_input('Ingrese etiqueta')

    search = col2.button('Buscar')

    if search and not conection and not tag:
        st.error('Verifica si estas conectado o introduce una etiqueta')
    elif tag and search:
        st.header('Ficheros:')
        list = ["henri","ale","airelys"]
        for i in list:
            c1, c2 = st.columns(2)
            c1.write(i)
            c2.button('Descargar',i)
            if c2 :
                pass


def page2 ():
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

def run():
    select_page = st.sidebar.selectbox('Seleccionar pagina', page_names.keys())
    page_names[select_page]()
    
run()