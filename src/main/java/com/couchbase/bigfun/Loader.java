package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;

import java.io.IOException;
import java.lang.*;
import java.util.Date;

public class Loader<PARAMT, DATAT> extends Thread {

        public static final String INSERT_OPERATION = "insert";
        public static final String DELETE_OPERATION = "delete";
        public static final String UPDATE_OPERATION = "update";
        public static final String TTL_OPERATION = "ttl";

        private LoadData data;

        private LoadParameter parameter;

        private LoadTarget target;

        public LoadStats successStats;

        public LoadStats failedStat;

        protected DATAT getData() {
            return (DATAT)data;
        }

        protected PARAMT getParameter() {
            return (PARAMT)parameter;
        }

        /*
        To be override
         */
        protected void load() {
            while (true) {
                try {
                    if (!operate(INSERT_OPERATION))
                        break;
                }
                catch (Exception e) {
                    System.err.println(e.toString());
                    continue;
                }
            }
            return;
        }

        public void run() {
            load();
        }

        protected boolean operate(String operation) {
            boolean result;
            switch (operation) {
                case INSERT_OPERATION:
                    result = this.insert();
                    break;
                case DELETE_OPERATION:
                    result = this.delete();
                    break;
                case UPDATE_OPERATION:
                    result = this.update();
                    break;
                case TTL_OPERATION:
                    result = this.ttl();
                    break;
                default:
                    result = false;
                    break;
            }
            return result;
        }

        private boolean ttl() {
            boolean result;
            try {
                JsonDocument doc = data.GetNextDocumentTTL();
                if (doc != null) {
                    this.target.upsert(doc);
                    this.successStats.ttlNumber++;
                    result = true;
                }
                else
                    result = false;
            }
            catch (Exception e) {
                this.failedStat.ttlNumber++;
                throw e;
            }
            return result;
        }

        private boolean insert() {
            boolean result;
            try {
                JsonDocument doc = data.GetNextDocumentForInsert();
                if (doc != null) {
                    this.target.upsert(doc);
                    this.successStats.insertNumber++;
                    result = true;
                }
                else
                    result = false;
            }
            catch (Exception e) {
                this.failedStat.insertNumber++;
                throw e;
            }
            return result;
        }

        private boolean delete() {
            boolean result;
            try {
                JsonDocument doc = data.GetNextDocumentForDelete();
                if (doc != null) {
                    this.target.delete(doc);
                    this.successStats.deleteNumber++;
                    result = true;
                }
                else
                    result = false;
            }
            catch (Exception e) {
                this.failedStat.deleteNumber++;
                throw e;
            }
            return result;
        }

        private boolean update() {
            boolean result;
            try {
                JsonDocument doc = data.GetNextDocumentForUpdate();
                if (doc != null) {
                    this.target.upsert(doc);
                    this.successStats.updateNumber++;
                    result = true;
                }
                else
                    result = false;
            }
            catch (Exception e) {
                this.failedStat.updateNumber++;
                throw e;
            }
            return result;
        }

        protected Loader(LoadParameter parameter, LoadData data, LoadTarget loadTarget) {
            super();
            this.parameter = parameter;
            this.data = data;
            if (loadTarget == null)
                this.target = new LoadTarget(this.parameter.targetInfo);
            else
                this.target = loadTarget;
            this.successStats = new LoadStats();
            this.failedStat = new LoadStats();
        }

        public Loader(LoadParameter parameter, LoadData data) {
            this(parameter, data, null);
        }
    }

