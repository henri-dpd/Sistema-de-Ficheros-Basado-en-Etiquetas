from PyQt5.QtWidgets import QApplication, QDialogButtonBox, QDialog, QLabel
from PyQt5.QtWidgets import QFileDialog, QTabWidget, QMainWindow, QListWidget
from PyQt5.QtWidgets import QListWidgetItem, QHBoxLayout, QVBoxLayout, QLineEdit
from PyQt5.QtWidgets import QWidget, QPushButton, QMessageBox
from PyQt5.QtCore import QSize
import client
import glob
import os
import sys

path = ''
tag = ''

class MainWindow(QMainWindow):
    def __init__(self, ip):
        super().__init__()

        self.client = client(ip)
        
        self.setWindowTitle("Cliente")
        self.setFixedSize(QSize(1290,768))

        self.layout = QVBoxLayout(self)

        #####################################################################################################

        self.layout_1 = QVBoxLayout()
        self.widget_1 = QWidget()
        self.widget_1.setLayout(self.layout_1)

        self.layout_label_ip = QHBoxLayout()
        self.widget_label_ip = QWidget()
        self.widget_label_ip.setLayout(self.layout_label_ip)
        self.label_ip = QLabel("Direccion ip:")
        self.layout_label_ip.addWidget(self.label_ip)
        self.layout_1.addWidget(self.widget_label_ip)

        self.layout_connect = QHBoxLayout()
        self.widget_connect = QWidget()
        self.widget_connect.setLayout(self.layout_connect)
        self.ip = QLineEdit()
        self.layout_connect.addWidget(self.ip)
        self.button_1 = QPushButton("Conectar")
        self.button_1.clicked.connect(self.connect_client)
        self.layout_connect.addWidget(self.button_1)
        self.layout_1.addWidget(self.widget_connect)

        self.layout_label_tags = QHBoxLayout()
        self.widget_label_tags = QWidget()
        self.widget_label_tags.setLayout(self.layout_label_tags)
        self.label_tags = QLabel("Etiquetas:")
        self.layout_label_tags.addWidget(self.label_tags)
        self.layout_1.addWidget(self.widget_label_tags)
        
        self.layout_tags = QHBoxLayout()
        self.widget_tags = QWidget()
        self.widget_tags.setLayout(self.layout_tags)
        self.tags = QLineEdit()
        self.layout_tags.addWidget(self.tags)
        self.button_2 = QPushButton("Buscar")
        self.button_2.clicked.connect(self.search_tag)
        self.layout_tags.addWidget(self.button_2)
        self.layout_1.addWidget(self.widget_tags)
         
        self.layout_label = QHBoxLayout()
        self.widget_label = QWidget()
        self.widget_label.setLayout(self.layout_label)
        self.label = QLabel("Ficheros:")
        self.layout_label.addWidget(self.label)
        self.layout_1.addWidget(self.widget_label)

        self.list_files = []
        self.layout_list = QHBoxLayout()
        self.widget_list = QWidget()
        self.widget_list.setLayout(self.layout_list)
        self.lv = QListWidget()
        self.lv.addItems(self.list_files)
        self.lv.itemSelectionChanged.connect(self.item_changed)
        self.lv.itemClicked.connect(self.send)
        self.layout_list.addWidget(self.lv)
        self.layout_1.addWidget(self.widget_list)

        #############################################################################################


        self.layout_2 = QVBoxLayout()
        self.widget_2 = QWidget()
        self.widget_2.setLayout(self.layout_2)

        self.layout_label_dir = QHBoxLayout()
        self.widget_label_dir = QWidget()
        self.widget_label_dir.setLayout(self.layout_label_dir)
        self.label_dir = QLabel("Ingrese directorio:")
        self.layout_label_dir.addWidget(self.label_dir)
        self.layout_2.addWidget(self.widget_label_dir)

        self.layout_search_file = QHBoxLayout()
        self.widget_search_file = QWidget()
        self.widget_search_file.setLayout(self.layout_search_file)
        self.line_search_file = QLineEdit()
        self.layout_search_file.addWidget(self.line_search_file)
        self.button_3 = QPushButton("...")
        self.button_3.clicked.connect(self.search_file)
        self.layout_search_file.addWidget(self.button_3)
        self.layout_2.addWidget(self.widget_search_file)

        self.layout_label_ip2 = QHBoxLayout()
        self.widget_label_ip2 = QWidget()
        self.widget_label_ip2.setLayout(self.layout_label_ip2)
        self.label_ip2 = QLabel("Direccion ip:")
        self.layout_label_ip2.addWidget(self.label_ip2)
        self.layout_2.addWidget(self.widget_label_ip2)

        self.layout_connect2 = QHBoxLayout()
        self.widget_connect2 = QWidget()
        self.widget_connect2.setLayout(self.layout_connect2)
        self.ip2 = QLineEdit()
        self.layout_connect.addWidget(self.ip2)
        self.layout_1.addWidget(self.widget_connect2)

        self.layout_label_up = QHBoxLayout()
        self.widget_label_up = QWidget()
        self.widget_label_up.setLayout(self.layout_label_up)
        self.label_up = QLabel("Ingrese etiquetas para el fichero a subir:")
        self.layout_label_up.addWidget(self.label_up)
        self.layout_2.addWidget(self.widget_label_up)

        self.layout_tag_up = QHBoxLayout()
        self.widget_tag_up = QWidget()
        self.widget_tag_up.setLayout(self.layout_tag_up)
        self.line_tag_up = QLineEdit()
        self.layout_tag_up.addWidget(self.line_tag_up)
        self.layout_2.addWidget(self.widget_tag_up)

        self.docs = []

        self.layout_label_f = QHBoxLayout()
        self.widget_label_f = QWidget()
        self.widget_label_f.setLayout(self.layout_label_f)
        self.label_f = QLabel("Ficheros a subir:")
        self.layout_label_f.addWidget(self.label_f)
        self.layout_2.addWidget(self.widget_label_f)

        self.layout_list_docs = QHBoxLayout()
        self.widget_list_docs = QWidget()
        self.widget_list_docs.setLayout(self.layout_list_docs)
        self.lv_2 = QListWidget()
        self.lv_2.addItems(self.docs)
        self.lv_2.itemSelectionChanged.connect(self.item_changed_2)
        self.lv_2.itemClicked.connect(self.send2)
        self.layout_list_docs.addWidget(self.lv_2)
        self.layout_2.addWidget(self.widget_list_docs)


        ############################################################################################
        
        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.West)
        self.tabs.setMovable(True)

        for i,widget in enumerate([self.widget_1,self.widget_2]):
            self.tabs.addTab(widget, 'page ' + str(i+1))

        self.layout.addWidget(self.tabs)
        self.setLayout(self.layout)

        self.setCentralWidget(self.tabs)

    ################################################################################################

    def item_changed(self):
        item = QListWidgetItem(self.lv.currentItem())

    def send(self):

        ip = self.ip.text()
        self.path = self.lv.currentItem().text()

        dlg = CustomDialog(self) 

        if dlg.exec():

            self.client.send_info(ip, 'send_file', {'path':self.path})

            dlg2 = QMessageBox(self)
            dlg2.setWindowTitle("Cliente")
            dlg2.setText("Descargandose")
            dlg2.exec()

            

        else: pass

    def item_changed_2(self):
        item = QListWidgetItem(self.lv_2.currentItem())
        
    def send2(self):
        global path, tag
        self.path = self.lv_2.currentItem().text()
        path = self.path
        tag = self.line_tag_up.text().split(' ')
        ip = self.ip2.text()

        dlg = CustomDialog2(self)

        if dlg.exec():

            self.client.send_info(ip, 'recv_file', {'path' : path, 'destination_address' : self.client.address, 'tags' : tag})

            dlg2 = QMessageBox(self)
            dlg2.setWindowTitle("Cliente")
            dlg2.setText("Subiendose")
            dlg2.exec()

            

        else: pass

    def connect_client(self):
        ip = self.ip.text()
        self.client.send_info(ip,'are_you_alive',{})


    def search_tag(self):
        ip = self.ip.text()
        tag =self.tags.text()

        self.list_files = []
        self.lv.addItems(self.list_files)
        self.lv.itemSelectionChanged.connect(self.item_changed)

        self.client.send_info(ip, 'get_tag', {'tag': tag})

        self.lv.addItems(self.list_files)
        self.lv.itemSelectionChanged.connect(self.item_changed)

    def search_file(self):
        self.docs = []
        self.lv_2.addItems(self.docs)
        self.lv_2.itemSelectionChanged.connect(self.item_changed_2)

        self.path = str(QFileDialog.getExistingDirectory())
        self.line_search_file.setText(self.path)
        self.docs = glob.glob(os.path.join(self.path + "/**"), recursive=True)[1:]


        self.lv_2.addItems(self.docs)
        self.lv_2.itemSelectionChanged.connect(self.item_changed_2)

##########################################################################################################

class CustomDialog(QDialog):
    def __init__(self, parent = None):
        super().__init__(parent)

        self.setWindowTitle("Cliente")

        qbutton = QDialogButtonBox.StandardButton.Ok| QDialogButtonBox.StandardButton.Cancel

        self.button_box = QDialogButtonBox(qbutton)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)

        self.layout_dl = QVBoxLayout()
        message = QLabel("Estas seguro que quieres descargarlo")
        self.layout_dl.addWidget(message)
        self.layout_dl.addWidget(self.button_box)
        self.setLayout(self.layout_dl)

######################################################################################################3

class CustomDialog2(QDialog):
    def __init__(self, parent = None):
        super().__init__(parent)

        self.setWindowTitle("Cliente")

        qbutton = QDialogButtonBox.StandardButton.Ok| QDialogButtonBox.StandardButton.Cancel

        self.button_box = QDialogButtonBox(qbutton)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)

        self.layout_dl = QVBoxLayout()
        message = QLabel("Estas seguro que quieres subir el fichero: " + path + 
                         " con las etiquetas: " + tag)
        self.layout_dl.addWidget(message)
        self.layout_dl.addWidget(self.button_box)
        self.setLayout(self.layout_dl)

########################################################################################################

def run(ip):

    app = QApplication(sys.argv)

    window = MainWindow(ip)
    window.show()

    app.exec()