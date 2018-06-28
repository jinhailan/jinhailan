#from mtk.utils import get_sundays
from mtk.utils.cmd import exec_cmd
from mtk.backup import XtraBackupDir
from pathlib import Path
import shutil 


#全局的变量
work_path='/data0/recover_work'
recover_path=Path(work_path)    
inr_dirs=XtraBackupDir(work_path).incr_dirs()
option="--redo-only --apply-log-only"
cmd_base="  --target-dir={}".format(recover_path / 'base')
#cmd_one_inr=" --incremental-basedir=/data0/recover_work/base  --target-dir={}".format(recover_path/inr_dirs[0])
cmd_inrs=" --target-dir={}  --incremental-dir={} " #.format(work_path/inr_dirs[index-2],work_path/inr_dirs[index-1])


def copy_backup_dir_ok(in_path,day):
    xbd= XtraBackupDir(in_path)
    back_path= XtraBackupDir(in_path).path
    if  recover_path.exists(): #判断recover目录是否存在，不存在的话创建
        print(recover_path)
    else:
        recover_path.mkdir(parents=True, exist_ok=True)
    if xbd.base.exists():       # 判断备份目录是否存在 
        shutil.copytree(xbd.base,'/data0/recover_work/base')
        for i in  xbd.incr_dirs(day):
            shutil.copytree(back_path/i,recover_path/i)
        return True
    else:
        print('false path:',back_path)
        return False


def recover():
    full_prepare= False
    inr_prepare = False
    dirs=XtraBackupDir(work_path)
    print(dirs.list_dirs())
    if 'base' in dirs.list_dirs():
        if len(dirs.list_dirs()) == 1:

            full_prepare =True
        elif len(dirs.list_dirs()) >1:
            inr_prepare=True
    else :
        print('no base in /data0/recover_work!')
        return False

    if full_recover:
        full_recover(port,recover_path)
    if inr_recover:
        inr_recover(port,recover_path)


def full_recover(port,recover_path): #执行全备恢复
    cmd_pre="--apply-log  --target-dir={}".format(recover_path / 'base')
    full_recover_cmd="xtrabackup  --datadir=/data0/recover_work/recover  --socket=/tmp/mysql_{}.sock\
     --copy-back   --target-dir={}".format(port,recover_path/ 'base')
    full_prepare_cmd=cmd_init+ cmd_pre
    print (full_prepare_cmd)
    rc,stdout, stderr= exec_cmd(full_prepare_cmd)
    if rc == 0:
        print('\033[1;31m***prepare is done!***\033[0m')
        print(full_recover_cmd)    
        rc,stdout, stderr= exec_cmd(full_recover_cmd)
        if rc == 0:
            print('\033[1;31m***recover is done!***\033[0m')
        else:
            print(stderr)
    else:
        print('prepare is failed!')





def inr_recover(port,day):
    full_prepare_cmd=cmd_init+option+cmd_base #执行全量prepare
    print (full_prepare_cmd)
    rc, stdout, stderr=exec_cmd(full_prepare_cmd)
    if rc != 0:
        print(stderr)
        return

    print('\033[1;31mfullbackup prepare is ok!\033[0m')
    inr_dirs.sort() 
    
    r = True 
    if len(inr_dirs) > 1:
        if mid(recover_path, inr_dirs[:-1],port) == False:
            return
             
    b = inr_dirs[-1]
    cmd = cmd_init+cmd_inrs.format(recover_path/'base' ,recover_path/ b)
    print(cmd)
    rc, stdout, stderr=exec_cmd(cmd)
    print(rc, stdout, stderr)
    if rc != 0:
        return

    print('\033[1;31mrecover is ok!\033[0m')
    full_recover_cmd="xtrabackup  --datadir=/data0/recover_work/recover  --socket=/tmp/mysql_{}.sock\
         --copy-back   --target-dir={}".format(port,recover_path/ 'base')
    print(full_recover_cmd)
    rc, stdout, stderr=exec_cmd(full_recover_cmd)
    print(rc,stderr)
    if rc == 0:
        print('\033[1;31mcopyback is complete:path: %s\033[0m'%(recover_path/'recover'))
        


def mid(recover_path, l,port):
    cmd_init="xtrabackup --prepare  --socket=/tmp/mysql_{}.sock ".format(port)
    for index, i in enumerate(l):
        cmd = cmd_init+option+cmd_inrs.format(recover_path/'base', recover_path/i)
        print(cmd)
        rc,stdout, stderr=exec_cmd(cmd)
        if rc == 0:
            print('\033[1;31minr_prepare is ok\033[0m')
            
        else:
            print('\033[1;31minr_prepare is failed\033[0m')
            return False
    return True        
    
   # return exec_cmd(cmd)
        



'''
if  __name__== '__main__' :
    in_path='/data0/backup/xtrabackup/2018-06-24_2018-06-30/3307/'
    day='2018-06-27'
    if recover_reday(in_path,day):
        recover()
        inr_recover(port,work_path)
#    prepare_cmd(work_path)

'''
work_path='/data0/recover_work'
port=3307
cmd_init="xtrabackup --prepare  --socket=/tmp/mysql_{}.sock ".format(port)
in_path='/data0/backup/xtrabackup/2018-06-24_2018-06-30/3307/'
day='2018-06-28'
inr_recover(port,day)

#full_recover(port,recover_path)



