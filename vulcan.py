#!/usr/bin/env python3
import os,sys
import fileinput
import argparse
from jsmin import jsmin


def replacein(inputfile):
    with fileinput.FileInput(inputfile, inplace=True) as file:
        for line in file:
            print(line.replace('elements-built.html', 'elements-built.html'), end='')
    with fileinput.FileInput(inputfile, inplace=True) as file:
        for line in file:
            print(line.replace('app.js', 'app.min.js'), end='')

def replaceout(inputfile):
    with fileinput.FileInput(inputfile, inplace=True) as file:
        for line in file:
            print(line.replace('elements-built.html', 'elements-built.html'), end='')
    with fileinput.FileInput(inputfile, inplace=True) as file:
        for line in file:
            print(line.replace('app.min.js', 'app.js'), end='')

def changedebug(mode):
    if mode == 'build':
        with fileinput.FileInput('Settings.py', inplace=True) as file:
            for line in file:
                print(line.replace('DEBUG = True', 'DEBUG = False'), end='')
    if mode == 'dev':
        with fileinput.FileInput('Settings.py', inplace=True) as file:
            for line in file:
                print(line.replace('DEBUG = False', 'DEBUG = True'), end='')


def vulcanize():
    os.system('rm -f easyweb/static/elements/elements-built.html')
    command = 'vulcanize easyweb/static/elements/elements-built.html --out-html easyweb/static/elements/elements-built.html'
    os.system(command)

def minimize():
    with open('easyweb/static/scripts/app.js') as js_file:
        minified = jsmin(js_file.read())
    with open('easyweb/static/scripts/app.min.js','w') as js_file:
        js_file.write(minified)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build","-b", help="vulcanize",action="store_true")
    parser.add_argument("--dev","-d", help="(de)vulcanize",action="store_true")
    args = parser.parse_args()

    if args.build:
        vulcanize()
        minimize()
        replacein('templates/activate.html')
        replacein('templates/login-public.html')
        replacein('templates/main-public.html')
        replacein('templates/reset.html')
        replacein('templates/signup.html')
        replacein('templates/404.html')
        replacein('templates/service-down.html')
        changedebug('build')
        #changeports('build')


    if args.dev:
        replaceout('templates/activate.html')
        replaceout('templates/login-public.html')
        replaceout('templates/main-public.html')
        replaceout('templates/reset.html')
        replaceout('templates/signup.html')
        replaceout('templates/404.html')
        replaceout('templates/service-down.html')
        changedebug('dev')
        #changeports('dev')
