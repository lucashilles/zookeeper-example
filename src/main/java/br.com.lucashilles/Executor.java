package br.com.lucashilles;
/*
    Um programa simples que implementa o DataMonitor para iniciar e parar
    executáveis com base em um znode. O programa observa o znode especificado e
    salva os dados que correspondem ao znode em um arquivo. Ele também inicia o
    programa especificado com os argumentos especificados quando o znode existe
    e encerra o programa se o znode desaparecer.
 */
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Executor
        implements Watcher, Runnable, DataMonitorListener
{
    /// Nome do znode que está sendo observado.
    String znode;

    DataMonitor dataMonitor;

    /// Instância de cliente do Zookeeper.
    ZooKeeper zooKeeper;

    /// Nome do arquivo no qual serão armazenados o dados recebidos.
    String filename;

    /// Processo e argumentos que serão executados ao verificar uma alteração do znode.
    String[] exec;
    Process child;

    public Executor(String hostPort, String znode, String filename,
                    String[] exec) throws IOException {
        this.znode = znode;
        this.filename = filename;
        this.exec = exec;
        zooKeeper = new ZooKeeper(hostPort, 3000, this);
        dataMonitor = new DataMonitor(zooKeeper, znode, null, this);
    }

    /**
     * @param args Argumentos da linha de comando no formato: hostPort znode filename program [args ...]
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err
                    .println("USAGE: Executor hostPort znode filename program [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = args[1];
        String filename = args[2];
        String[] exec = new String[args.length - 3];
        System.arraycopy(args, 3, exec, 0, exec.length);
        try {
            new Executor(hostPort, znode, filename, exec).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    ///
    /// Pode ser realizado o processamento das informações, mas o evento deve ser passado a diante.
    ///
    public void process(WatchedEvent event) {
        dataMonitor.process(event);
    }

    public void run() {
        try {
            synchronized (this) {
                while (!dataMonitor.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            System.err.println("[EXECUTOR::run] " + e.getMessage());
        }
    }

    public void closing(int reasonCode) {
        synchronized (this) {
            notifyAll();
        }
    }

    static class StreamWriter extends Thread {
        OutputStream os;

        InputStream is;

        StreamWriter(InputStream is, OutputStream os) {
            this.is = is;
            this.os = os;
            start();
        }

        public void run() {
            byte[] b = new byte[80];
            int rc;
            try {
                while ((rc = is.read(b)) > 0) {
                    os.write(b, 0, rc);
                }
            } catch (IOException e) {
                System.err.println("[STREAMWRITER::run] ERROR - " + e.getMessage());
            }

        }
    }

    public void exists(byte[] data) {
        /// Se os dados forem nulos o znode foi excluido.
        if (data == null) {
            if (child != null) {
                System.out.println("Matando o processo");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    System.err.println("[EXECUTOR::exists] " + e.getMessage());
                }
            }
            child = null;
        } else {
            if (child != null) {
                System.out.println("Parando o processo filho");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileOutputStream fos = new FileOutputStream(filename);
                fos.write(data);
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.out.println("Iniciando processo filho: " + Arrays.toString(exec));
                child = Runtime.getRuntime().exec(exec);
                new StreamWriter(child.getInputStream(), System.out);
                new StreamWriter(child.getErrorStream(), System.err);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}