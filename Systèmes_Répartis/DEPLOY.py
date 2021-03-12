import pandas as pd
import subprocess as sp
import multiprocessing as mp
import time


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
        


def copieSurMachineDeSlave(machine, fichier="/Users/amandineguignard/tmp/amichalewicz/SLAVE.py"):
    try:
        commandShell = 'ssh amichalewicz@' + machine + ' mkdir -p /tmp/amichalewicz/'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        commandShell  = 'scp ' + fichier +' amichalewicz@' + machine + ':/tmp/amichalewicz'
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
            print("Copie ok sur la machine " + machine, end='\n')
        pass        

        
if __name__ == '__main__':
    machines = ListeMachines("/Users/amandineguignard/tmp/amichalewicz/machineSshDispo.txt")

    #Vérification de l'état des machines (en parallèle)
    machinesTemp = []
    start = time.time()
    with mp.Pool(10) as p:
        machinesTemp = p.map(rechercheMachinesOn, machines)

    #On ne garde que les machines allumées
    machinesAllumees = []
    for val in machinesTemp:
        if val != None:
            machinesAllumees.append(val)
    print('Nombre de machines allumées : ' + str(len(machinesAllumees)))
    print('Trouvées en ' + str(time.time() - start) + ' s', end='\n\n')

    #Copie du fichier Slave.py sur toutes les machines allumées (en parallèle)
    print('Copie des fichiers sur les machines allumées')

    with mp.Pool(10) as p:
        p.map(copieSurMachineDeSlave, machinesAllumees)
    print('')
    print('Fin de la copie des fichiers', end='\n\n')