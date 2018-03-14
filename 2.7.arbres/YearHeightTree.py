from __future__ import unicode_literals
from hdfs3 import HDFileSystem

test_host = 'localhost'
test_port = 9000

class Tree:
    def __init__(self, geo_point, arrondi, genre, espace, famille, annee, hauteur, circonf, adresse, commun, variete, obj, nom_ev):
        self.geo_point = geo_point
        self.arrondi = arrondi
        self.genre = genre
        self.espace = espace
        self.famille = famille
        self.annee = annee
        self.hauteur = hauteur
        self.circonf = circonf
        self.adresse = adresse
        self.commun = commun
        self.variete = variete
        self.obj = obj
        self.nom_ev = nom_ev
    def display_YHTree(self):
        print("Year:", self.annee, " Height:", self.hauteur)

def ReadFile(hdfs_client, file):
    with hdfs_client.open(file, 'rb') as f:
        lines = f.readlines()
    return lines

if __name__ == '__main__':
    
    file = '/test/arbres.csv'

    # connect to HDFS and read the file
    hdfs_client = HDFileSystem(host=test_host, port=test_port)
    lines = ReadFile(hdfs_client, file)
    hdfs_client.disconnect()
    
    # convert the lines into tree info
    tree = []
    n = len(lines) - 1
    for i in range(n):
        info_tree = lines[i+1].split(b';')
        geo_point = info_tree[0]
        arrondi = info_tree[1]
        genre = info_tree[2]
        espace = info_tree[3]
        famille = info_tree[4]
        annee = info_tree[5]
        hauteur = info_tree[6]
        circonf = info_tree[7]
        adresse = info_tree[8]
        commun = info_tree[9]
        variete = info_tree[10]
        obj = info_tree[11]
        nom_ev = info_tree[12][0:-2]
        # build a tree class
        tree.append(Tree(geo_point, arrondi, genre, espace, famille, annee, hauteur, circonf, adresse, commun, variete, obj, nom_ev))
    
    print("There are", n, "trees :")
    for i in range(n):
        print(i, end=' ')
        tree[i].display_YHTree()
    
    print("-" * 20 + "END" + "-" * 20)
