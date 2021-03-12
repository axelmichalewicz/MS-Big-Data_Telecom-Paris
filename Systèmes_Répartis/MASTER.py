import subprocess as sp
import pandas as pd
import time
import multiprocessing as mp
import os, os.path

def ListeMachines(file):
    listMachines = pd.read_csv(file)
    return  listMachines["Machines"].values.tolist()

def rechercheMachinesOn(machine):
    commandShell = 'ssh amichalewicz@' + machine + ' hostname'
    try:
        monProcess = sp.run(commandShell, shell=True, capture_output=True, timeout=10)
    except sp.TimeoutExpired:
        print("machine " + machine + " éteinte")
        pass
    else:
        #Si connexion à la machine
        if not monProcess.returncode:
            print("machine " + machine + " allumée")
            return machine
        #Sinon
        else:
            print("Problème sur la machine " + machine)
            pass

def runSlaveOn(fichier, machine, option):
    try:
        #time = 15
        #Etape 5
        #subprocess.run(["ls", "-al", "/tmp"], capture_output=False,timeout=time)
        #a rajouter la notion d'erreur
        #subprocess.run(["python", "/Users/amandineguignard/tmp/amichalewicz/Slave.py"], capture_output=False, timeout=time)
        
        if option==0:
            commandShell = 'ssh amichalewicz@' + machine + ' python3 /tmp/amichalewicz/SLAVE.py 0 /tmp/amichalewicz/splits/' + fichier
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        elif option==1:
            commandShell = 'ssh amichalewicz@' + machine + ' python3 /tmp/amichalewicz/SLAVE.py 1 /tmp/amichalewicz/maps/' + fichier
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        elif option==2:
            commandShell = 'ssh amichalewicz@' + machine + ' python3 /tmp/amichalewicz/SLAVE.py 2 /tmp/amichalewicz/'
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        else:
            print('unknown option')
    except sp.TimeoutExpired:
        print("echec d'execution de SLAVE.py dû au timeout", end='\n')
        pass
    else:
    #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Problème avec l'exécution de SLAVE.py sur la machine " + machine)
            print(monProcess.stderr)
        #Sinon
        else:
            print("Fin d'execution de SLAVE.py sur la machine " + machine, end='\n')
        pass 

            

def copySplits(fichier, machine):
    try:
        commandShell = 'ssh amichalewicz@' + machine + ' mkdir -p /tmp/amichalewicz/splits'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        commandShell  = 'scp ' + fichier +' amichalewicz@' + machine + ':/tmp/amichalewicz/splits'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
    except sp.TimeoutExpired:
        print("machine" + machine + " éteinte")
        pass
    else:
        #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Problème de copie avec la machine " + machine)
            print(monProcess.stderr)
        #Sinon
        else:
            print("Copie ok du fichier " +fichier+" sur la machine " + machine, end='\n')
        pass

def copyFichierNbMachine(machineTP):
    try:
        commandShell  = 'scp /Users/amandineguignard/tmp/amichalewicz/machine.txt amichalewicz@' + machineTP + ':/tmp/amichalewicz'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
    except sp.TimeoutExpired:
        print("machine" + machineTP + " éteinte")
        pass
    else:
        #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Problème de copie du fichier machine.txt avec la machine " + machineTP)
            print(monProcess.stderr)
        #Sinon
        else:
            print("Copie ok du fichier machine.txt sur la machine " + machineTP, end='\n')
        pass        

def creation_splits(chemin, file):
    texteALire= open(file, 'r')
    i=0
    for w in texteALire.readlines():
        if w!='\n' and w!='':
            fichier='S'+str(i)+'.txt'
            if w[-1:]=='\n':
                with open(chemin+'/'+ fichier, 'w') as f:
                    f.write(w[:-1])
            else:
                with open(chemin+'/'+ fichier, 'w') as f:
                    f.write(w)
            i+=1

def split(chemin):
    try:
        commandShell = 'mkdir -p ' + chemin + '/splits'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        creation_splits(chemin + '/splits', chemin+'/input.txt')
    except sp.TimeoutExpired:
        print("Impossible de creer le dossier et les fichiers splits")
        pass
    else:
        if monProcess.returncode:
            print("Probleme d'ecriture des dossiers et fichiers splits sur la machine ")
            print(monProcess.stderr)
        #Sinon
        else:
            print("ecriture des splits ok sur la machine ")
        pass

def copyReduces(machine):
    try:
        commandShell  = 'scp -r amichalewicz@' + machine + ':/tmp/amichalewicz/reduces/ /Users/amandineguignard/tmp/amichalewicz'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=4)        
    except sp.TimeoutExpired:
        print(machine+" éteinte")
        pass
    else:
        #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Pas de dossier reduces dans la machine " + machine)
            print(monProcess.stderr)
        #Sinon
        else:
            print("Copie ok des fichiers reduces sur ma machine ", end='\n')
        pass


def afficheReduce(chemin):
    if os.path.exists(chemin):
        result=[]
        try:
            for item in os.listdir(chemin): 
                texteALire= open(chemin+'/'+item, 'r', errors='ignore')
                for w in texteALire.readlines():
                    if len(w.split(" "))<3:
                        if w[-1:]=='\n':
                            print(w[:-1])
                        else:
                           print(w)
                
        except sp.TimeoutExpired:
            print("Impossible d'accéder au dossier reduce")
            pass
    return result


if __name__ == '__main__':
    machines = ListeMachines("/Users/amandineguignard/tmp/amichalewicz/machineSshDispo.txt")
    a = time.time()


    #Vérification de l'état des machines (en parallèle) Vu en kitDataScience
    machinesTemp = []
    start = time.time()
    with mp.Pool(10) as p:
        machinesTemp = p.map(rechercheMachinesOn, machines)

    #On ne garde que les machines allumées
    machinesAllumees = []
    for val in machinesTemp:
        if val != None:
            machinesAllumees.append(val)
    
    machinesPourCopie=[]
    fichierACopier=[]
    fichierMachine=[]
    fichierAMapper=[]
    fichierMapMachine=[]
    
    chemin='/Users/amandineguignard/tmp/amichalewicz/'
    split(chemin)
    chemin='/Users/amandineguignard/tmp/amichalewicz/splits/'
    
    #nb de fichier à copier dans sur chaque machine
    nbFiles = len([item for item in os.listdir(chemin) if os.path.isfile(os.path.join(chemin, item)) and item !='.DS_Store'])
    #liste des fichier Splità copier
    for item in os.listdir(chemin): 
        if os.path.isfile(os.path.join(chemin, item)):
            if item !='.DS_Store':
                fichierACopier.append(chemin+item)
                fichierAMapper.append(item)
    
    ##liste des fichiers à copier sans le chemin
    #for item in os.listdir(chemin): 
        #if os.path.isfile(os.path.join(chemin, item)):
            #if item !='.DS_Store':
                #fichierAMapper.append(item)
    
    #création du vecteur de machines allumées sur lequel on va copier les fichiers        
    j=0
    for i in range(0,nbFiles):
        if i < len(machinesAllumees):
            machinesPourCopie.append(machinesAllumees[i])
        else:
            if j < len(machinesAllumees):
                machinesPourCopie.append(machinesAllumees[j])
                j+=1
            else: 
                j=0
                machinesPourCopie.append(machinesAllumees[j])
                j+=1
    
    #écriture du fichier machine.txt
    with open('/Users/amandineguignard/tmp/amichalewicz/machine.txt', 'w') as f:
        for value in machinesPourCopie:
            f.write('{}'.format(value) + '\n')
    
    #création du map fichier avec chemin + machine pour la copie des splits sur chaque machine de l'école
    for i in range(0,nbFiles):
        fichierMachine.append([])
        fichierMachine[i]=fichierACopier[i],machinesPourCopie[i]
    
    #création du map nom du fichier + machine pour le map sur chaque machine de l'école
    for i in range(0,nbFiles):
        fichierMapMachine.append([fichierAMapper[i],machinesPourCopie[i]])
        #fichierMapMachine[i]=fichierAMapper[i],machinesPourCopie[i]

    b = time.time()
    
    with mp.Pool(10) as p:
        p.starmap(copySplits, fichierMachine)  
    
    print('Fin de copie des splits')
    
    c = time.time()

    with mp.Pool(10) as p:
        p.map(copyFichierNbMachine, machinesPourCopie)  
    
    print('Fin de copie des fichiers machine.txt')
    
    d = time.time()
    
    
    #création du tuples pour le starmap du Slave option 0 = Map
    fichierMachineOpt0=[]
    for i in range(0,len(fichierMapMachine)):
        fichierMachineOpt0.append(fichierMapMachine[i][:])
        fichierMachineOpt0[i].append(0)
        fichierMachineOpt0[i]=tuple(fichierMachineOpt0[i])   
    
        
    with mp.Pool(10) as p:
        p.starmap(runSlaveOn, fichierMachineOpt0)

    
    e = time.time()
    
    print('')
    print('MAP FINISHED', end='\n\n')
    
     
    #création du tuples pour le starmap du Slave option 1 = Shuffle
    fichierMachineOpt1=[]
    for i in range(0,len(fichierMapMachine)):
        fichierMachineOpt1.append(fichierMapMachine[i][:])
        fichierMachineOpt1[i].append(1)
        fichierMachineOpt1[i][0]='UM'+fichierMachineOpt1[i][0][1:]
        fichierMachineOpt1[i]=tuple(fichierMachineOpt1[i])   
    

    with mp.Pool(10) as p:
        p.starmap(runSlaveOn, fichierMachineOpt1)
        
    f = time.time()
    
    print('')
    print('SHUFFLE FINISHED', end='\n\n')
    
    #création du tuples pour le starmap du Slave option 2 = reduce
    fichierMachineOpt2=[]
    for i in range(0,len(machinesPourCopie)):
        fichierMachineOpt2.append(['0'])
        fichierMachineOpt2[i].append(machinesPourCopie[i])
        fichierMachineOpt2[i].append(2)
        fichierMachineOpt2[i]=tuple(fichierMachineOpt2[i])    
    

    with mp.Pool(10) as p:
        p.starmap(runSlaveOn, fichierMachineOpt2)
    
    g = time.time()
    
    print('')
    print('REDUCE FINISHED', end='\n\n')
    
    with mp.Pool(10) as p:
        p.map(copyReduces, machinesPourCopie)
    
    h = time.time()
    
    print('')
    print ('Temps de connexion et recherche dess machines allumées:', (b-a)*1000,"ms")
    print ('Temps de copie des splits sur les machines allumées:', (c-b)*1000,"ms")
    print ('Temps de copie des fichiers machine.txt:', (d-c)*1000,"ms")
    print ('Temps du MAP:', (e-d)*1000,"ms") 
    print ('Temps du SHUFFLE:', (f-e)*1000,"ms")
    print ('Temps du REDUCE:', (g-f)*1000,"ms")
    print ("Temps du rapatriement des fichiers reduces et de l'affichage Resultat :", (h-g)*1000,"ms", end='\n\n')
    
    #afficheReduce('/Users/amandineguignard/tmp/amichalewicz/reduces')
    
    

    