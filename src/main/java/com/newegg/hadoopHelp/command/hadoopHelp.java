package com.newegg.hadoopHelp.command;

import com.newegg.hadoopHelp.service.HService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;

@ShellComponent
public class hadoopHelp {
    @Autowired
    HService hService;

    @ShellMethod("existPath")
    public void exist(@ShellOption("--user") String user,@ShellOption("--fsDefaultName") String fsDefaultName,@ShellOption("--path") String path){
        String exist = hService.exist(user, fsDefaultName, path);
        System.out.println(exist);
    }

    @ShellMethod("mkdir")
    public void mkdir(@ShellOption("--user") String user,@ShellOption("--fsDefaultName") String fsDefaultName,@ShellOption("--path") String path){
        try {
            hService.mkdir(user, fsDefaultName, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ShellMethod("uploadFile")
    public void uploadFile(@ShellOption("--user") String user,@ShellOption("--fsDefaultName") String fsDefaultName,@ShellOption("--srcPath") String srcpath,@ShellOption("--dstPath") String dstpath){
        try {
            hService.uploadFile(user, fsDefaultName, srcpath,dstpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ShellMethod("delete")
    public void delete(@ShellOption("--user") String user,@ShellOption("--fsDefaultName") String fsDefaultName,@ShellOption("--path") String path){
        try {
            hService.delete(user, fsDefaultName, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ShellMethod("download")
    public void download(@ShellOption("--user") String user,@ShellOption("--fsDefaultName") String fsDefaultName,@ShellOption("--srcPath") String srcpath,@ShellOption("--dstPath") String dstpath){
        try {
            hService.download(user, fsDefaultName, srcpath,dstpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
