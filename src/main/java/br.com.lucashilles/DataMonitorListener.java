package br.com.lucashilles;

/**
 * Outras classes podem utilizar o DataMonitor pela implementação desta interface.
 */
public interface DataMonitorListener {
    /**
     * Executado quando o estado o znode for alterado.
     */
    void exists(byte[] data);

    /**
     * A sessão do ZooKeeper não é mais válida.
     *
     * @param reasonCode Código de motivo do Zookeeper.
     */
    void closing(int reasonCode);
}
