#!/usr/bin/env python3
#import time
import sys, os
import subprocess as sp
import socket as sck
import os, os.path
import zlib

def count_words(file):
    texteALire= open(file, 'r')
    text=texteALire.read()
    listWords=[]
    i=0
    for words in text.split():
        listWords.append([])
        listWords[i]=words,1
        i+=1

    return listWords

def count_words_Reduced(file):
    texteALire= open(file, 'r')
    text=texteALire.read()
    dicoWords=dict()
    for words in text.split():
        if words in dicoWords:
            dicoWords[words]+=1
        else :
            dicoWords[words]=1

    return dicoWords

#compte le nombre d'occurence de chaque mot et reecrit chaque fichier avec mot occurence
def words_Reduced(file):
    text= open(file, 'r')
    dicoWords=dict()
    for words in text.readlines():
        w=words.split(' ')[0]
        if w in dicoWords:
            dicoWords[w]+=1
        else :
            dicoWords[w]=1
            
    with open(file, 'w') as f:
        for key, value in dicoWords.items():
            f.write('{} {}'.format(key, value) + '\n')
        
#ouvre et met tous les shufflesreceived dans un meme fichier
def open_Copy_Shuffle(file):
    texteALire= open(file, 'r')
    words=[]
    for w in texteALire.readlines():
        if w[-1:]=='\n':
            words.append(w[:-1])
        else:
            words.append(w)
    return words


def write_map(text,chemin,i):
    try:
        commandShell = 'mkdir -p ' + chemin + '/maps'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        chemin= chemin + '/maps'
        fichier='UM'+i+'.txt'
        liste=count_words(text)
        with open(chemin+'/'+ fichier, 'w') as f:
            for key, value in liste:
                f.write('{} {}'.format(key, value) + '\n')

    except sp.TimeoutExpired:
        print("Impossible de creer le fichier map")
        pass
    else:
        if monProcess.returncode:
            print("Probleme d'ecriture du fichier avec la machine ")
            print(monProcess.stderr)
        #Sinon
        else:
            print("ecriture ok du fichier " + fichier + " sur la machine ")
        pass
    
def prepareAndSendShuffle(fileUM,chemin):
    try:
        commandShell = 'mkdir -p ' + chemin + '/shuffles'
        monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
        nomMachine=sck.gethostname()
        with open(fileUM, 'r') as f:
            hashFile=[]
            for line in f:
                word=zlib.adler32(bytearray(line.split(sep=' ')[0],'utf8'))
                #si le fichier existe
                if os.path.isfile(chemin + '/shuffles/'+ str(word)+'-'+nomMachine+'.txt'):
                    with open(chemin + '/shuffles/'+ str(word)+'-'+nomMachine+'.txt', 'a') as file:
                        file.write('{} {}'.format(line.split(sep=' ')[0],line.split(sep=' ')[1][0]) + '\n')
                else :
                    with open(chemin + '/shuffles/'+ str(word)+'-'+nomMachine+'.txt', 'w') as file:
                        file.write('{} {}'.format(line.split(sep=' ')[0],line.split(sep=' ')[1][0]) + '\n')
                        hashFile.append(word)
        
        for h in hashFile:
            numeroMachine = h % len(countMachinesDispo(chemin+'/machine.txt'))
            copieSurMachine=countMachinesDispo(chemin+'/machine.txt')[numeroMachine]
            
            commandShell = 'ssh amichalewicz@' + copieSurMachine + ' mkdir -p /tmp/amichalewicz/shufflesreceived'
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
            commandShell  = 'scp ' + chemin + '/shuffles/'+ str(h)+ '-' +nomMachine+'.txt amichalewicz@' + copieSurMachine + ':/tmp/amichalewicz/shufflesreceived'
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10)
    
                        
    except sp.TimeoutExpired:
        print("Impossible de creer le dossier shuffles")
        pass
    else:
        if monProcess.returncode:
            print("Probleme d'ecriture du fichier avec la machine ")
            print(monProcess.stderr)
        #Sinon
        else:
            print("ecritures et envois ok des fichiers shuffles sur la machine ")
        pass 


def reduceFile(chemin):
    if os.path.exists(chemin+'/shufflesreceived/'):
        try:
            commandShell = 'mkdir -p ' + chemin + '/reduces'
            monProcess = sp.run(commandShell , shell=True, capture_output=True, timeout=10) 
            filesReduce=[]
        
            for item in os.listdir(chemin+'/shufflesreceived/'): 
                word_Reduce=open_Copy_Shuffle(chemin+'/shufflesreceived/'+item)
                if os.path.isfile(chemin+'/reduces/'+item.split('-')[0]+'.txt'):
                    with open(chemin+'/reduces/'+item.split('-')[0]+'.txt', 'a') as file:
                        for l in word_Reduce:
                            file.write(l + '\n')
                else :
                    filesReduce.append(chemin+'/reduces/'+item.split('-')[0]+'.txt')
                    with open(chemin+'/reduces/'+item.split('-')[0]+'.txt', 'w') as file:
                        for l in word_Reduce:
                            file.write(l + '\n')
            
            for i in filesReduce:
                if os.path.isfile(i):
                    words_Reduced(i)
    
        except sp.TimeoutExpired:
            print("Impossible de creer le fichier reduce")
            pass
        else:
            if monProcess.returncode:
                print("Probleme d'ecriture du fichier reduce avec la machine ")
                print(monProcess.stderr)
            #Sinon
            else:
                print("ecriture ok du dossier et des fichiers reduces")
            pass

def countMachinesDispo(file):
    machinesShuffle=[]
    with open(file, 'r') as f:
        for line in f:
            machinesShuffle.append(line[:-1])
            
    return machinesShuffle
    

def main():
    option = sys.argv[1]
    filename = sys.argv[2]
    x=filename[26:-4]
    pathname = os.path.dirname(sys.argv[0])
    if option == '0':
        write_map(filename,pathname,x)
    elif option == '1':
        prepareAndSendShuffle(filename,pathname)
    elif option == '2':
        reduceFile(pathname)
    else:
        print ('unknown option: ') + option
        sys.exit(1)

if __name__ == '__main__':
    main()
    
    

