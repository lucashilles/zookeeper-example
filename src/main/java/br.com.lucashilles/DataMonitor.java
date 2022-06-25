package br.com.lucashilles;
/*
  Classe que monitora os dados e a existencia de uma nó do Zookeeper
  Ela utiliza a API assíncrona do Zookeeper.
 */
import java.util.Arrays;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class DataMonitor implements Watcher, StatCallback {

    ZooKeeper zooKeeper;
    String znode;
    Watcher chainedWatcher;
    boolean dead;
    DataMonitorListener listener;
    byte[] prevData;

    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
                       DataMonitorListener listener) {
        this.zooKeeper = zk;
        this.znode = znode;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        /// Inicia por verificar se o nó existe, será totalmente orientados a eventos.
        zk.exists(znode, true, this, null);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            /// O evento informa que o estado da conexão mudou.
            switch (event.getState()) {
                case SyncConnected:
                    /// Os observadores serão registrados novamente automaticamente
                    /// e qualquer evento enviado enquando o cliente esteve desconectado
                    /// será recebido em ordem.
                    break;
                case Expired:
                    // Conexão expirou e o watcher foi morto.
                    dead = true;
                    listener.closing(Code.SESSIONEXPIRED.intValue());
                    break;
            }
        } else {
            if (path != null && path.equals(znode)) {
                /// Houve uma alteração no znode, busca os dados.
                zooKeeper.exists(znode, true, this, null);
            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

    public void processResult(int reasonCode, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (Code.get(reasonCode)) {
            case OK:
                exists = true;
                break;
            case NONODE:
                exists = false;
                break;
            case SESSIONEXPIRED:
            case NOAUTH:
                dead = true;
                listener.closing(reasonCode);
                return;
            default:
                zooKeeper.exists(znode, true, this, null);
                return;
        }

        byte[] b = null;
        if (exists) {
            try {
                b = zooKeeper.getData(znode, false, null);
            } catch (KeeperException e) {
                /// Qualquer erro será tratado pelo callback do observador.
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
        if ((b == null && b != prevData)
                || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
        }
    }
}