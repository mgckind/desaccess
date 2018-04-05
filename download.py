import os
import subprocess

import tornado.web


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera").decode('utf-8')


class DownloadCoaddObjectHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        pngName = self.get_argument("title")
        path = self.get_argument("path")
        jobid = self.get_argument("jobid")
        username = self.current_user.replace('\"', '')
        app_dir = os.path.dirname(__file__)
        tarName = pngName + '.tar.gz'
        filegz = path.replace('.tif.png', '.tar.gz')
        tarPath = os.path.join(app_dir, 'easyweb/static/workdir', username, jobid, tarName)
        dir_name = os.path.dirname(path)
        print("==>> pngName: ", pngName)
        print("==>> path: ", path)
        print("==>> jobid: ", jobid)
        print("==>> username: ", username)
        print("==>> app_dir: ", app_dir)
        print("==>> tarName: ", tarName)
        print("==>> filegz: ", filegz)
        print("==>> tarPath: ", tarPath)
        # get all files
        if os.path.isfile(tarPath):
            self.set_status(200)
            self.flush()
        else:
            p = '/' + path[path.find('easyweb'):path.find(os.path.basename(path))]
            print('========p: ', p)
            os.chdir(app_dir + p)
            print (os.getcwd())
            file_reg = pngName + '*'
            subprocess.check_call("tar -zcf {} {}".format(tarName, file_reg), shell=True)
            os.chdir(app_dir)
            self.set_status(200)
            self.flush()
        self.finish()


class DownloadEpochObjectHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        pngName = self.get_argument("title")
        path = self.get_argument("path")
        jobid = self.get_argument("jobid")
        # jobid_short = siid[:6]
        username = self.current_user.replace('\"', '')
        app_dir = os.path.dirname(__file__)
        archiveFolder = os.path.join(app_dir, 'easyweb/static/workdir', username, jobid)
        tarName = os.path.split(path)[1] + '.tar.gz'
        tarPath = os.path.join(archiveFolder, tarName)

        print("==>> pngName: ", pngName)
        print("==>> path: ", path)
        print("==>> jobid: ", jobid)
        print("==>> username: ", username)
        print("==>> app_dir: ", app_dir)
        print("==>> archiveF: ", archiveFolder)
        print("==>> tarName: ", tarName)
        print("==>> tarPath: ", tarPath)

        if os.path.exists(tarPath):
            self.set_status(200)
            self.flush()
        else:
            os.chdir(archiveFolder)
            print(os.getcwd())
            subprocess.check_call("tar -zcf {} {}".format(tarName, pngName), shell=True)
            os.chdir(app_dir)
            self.set_status(200)
            self.flush()
        self.finish()

class DownloadEpochSingleHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        pngName = self.get_argument("title")
        path = self.get_argument("path")
        jobid = self.get_argument("jobid")
        username = self.current_user.replace('\"', '')
        app_dir = os.path.dirname(__file__)
        tarName = pngName.replace('.png', '.tar.gz')
        filegz = path.replace('.tif.png', '.tar.gz')
        folder = pngName[:pngName.find('_')]
        tarPath = os.path.join(app_dir, 'easyweb/static/workdir', username, jobid, folder, tarName)
        dir_name = os.path.dirname(path)
        print("==>> pngName: ", pngName)
        print("==>> path: ", path)
        print("==>> jobid: ", jobid)
        print("==>> username: ", username)
        print("==>> app_dir: ", app_dir)
        print("==>> tarName: ", tarName)
        print("==>> filegz: ", filegz)
        print("==>> tarPath: ", tarPath)
        # get all files
        if os.path.isfile(tarPath):
            self.set_status(200)
            self.flush()
        else:
            p = '/' + path[path.find('easyweb'):path.find(os.path.basename(path))]
            print('========p: ', p)
            os.chdir(app_dir + p)
            print (os.getcwd())
            file_reg = pngName[:pngName.find('.png')] + '*'
            subprocess.check_call("tar -zcf {} {}".format(tarName, file_reg), shell=True)
            os.chdir(app_dir)
            self.set_status(200)
            self.flush()
        self.finish()