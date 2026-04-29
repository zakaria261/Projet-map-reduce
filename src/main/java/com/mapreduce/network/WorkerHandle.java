package com.mapreduce.network;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Représente un Worker enregistré du point de vue du Master.
 *
 * Contient les informations nécessaires pour que le Master
 * puisse contacter le Worker (PING, MAP_TASK, REDUCE_TASK).
 */
public class WorkerHandle {

    /** Adresse IP ou hostname du Worker. */
    public final String host;

    /** Port sur lequel le Worker écoute les tâches. */
    public final int port;

    /** Identifiant lisible : "host:port". */
    public final String id;

    /** true si le Worker répond aux PING. Volatile pour la visibilité entre threads. */
    private final AtomicBoolean alive = new AtomicBoolean(true);

    /** Timestamp du dernier PONG reçu (System.currentTimeMillis). */
    private volatile long lastSeen;

    public WorkerHandle(String host, int port) {
        this.host     = host;
        this.port     = port;
        this.id       = host + ":" + port;
        this.lastSeen = System.currentTimeMillis();
    }

    public boolean isAlive()  { return alive.get(); }
    public void setDead()     { alive.set(false); }
    public long getLastSeen() { return lastSeen; }

    public void markSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    @Override
    public String toString() { return id; }
}
