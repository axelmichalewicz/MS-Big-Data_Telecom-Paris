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


def nettoyageMachine(machine):
    try:
        commandShell = 'ssh amichalewicz@' + machine + ' rm -rf /tmp/amichalewicz/'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
    except sp.TimeoutExpired:
        print("machine" + machine + " éteinte")
        pass
    # Si la commande a renvoyé un résultat
    else:
        #Si on a reçu un message d'erreur
        if monProcess.returncode:
            print("Problème de nettoyage avec la machine " + machine)
            print(monProcess.stderr)
        #Sinon
        else:
            print("nettoyage du répertoire /tmp/amichalewicz/ sur la machine " + machine, end='\n')
        pass        


if __name__ == '__main__':
    machines = ListeMachines("/Users/amandineguignard/tmp/amichalewicz/machineSshDispo.txt")

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
    print('Nombre de machines allumées : ' + str(len(machinesAllumees)))
    print('Trouvées en ' + str(time.time() - start) + ' s', end='\n\n')
    print('nettoyage du répertoire /tmp/amichalewicz/ sur les machines allumées')
 
    with mp.Pool(10) as p:
        p.map(nettoyageMachine, machinesAllumees)
    print('')
    print('fin du nettoyage du répertoire /tmp/amichalewicz/ sur toutes les machines allumées', end='\n\n')

