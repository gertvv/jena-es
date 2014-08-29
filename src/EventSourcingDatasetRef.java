/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.server.CounterName;
import org.apache.jena.fuseki.server.CounterSet;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.server.ServiceRef;

/**
 * Hack DatasetRef to add the "history" service.
 */
public class EventSourcingDatasetRef extends DatasetRef
{
    public ServiceRef history                     = new ServiceRef("history") ;
    public ServiceRef delta                     = new ServiceRef("delta") ;
    
    private Map<String, ServiceRef> endpoints   = new HashMap<String, ServiceRef>() ;
    private boolean initialized = false ;
    
    // Two step initiation (c.f. Builder pattern)
    // Create object - incrementally set state - call init to calculate internal datastructures.
    public EventSourcingDatasetRef() {}
    public void init() {
        if ( initialized )
            Fuseki.serverLog.warn("Already initialized: dataset = "+name) ;
        initialized = true ;
        initServices() ;
    }
    
    @Override public String toString() { return "DatasetRef:'"+name+"'" ; }  
    
    private void initServices() {
        add(query) ;
        add(update) ;
        add(upload) ;
        add(readGraphStore) ;
        add(readWriteGraphStore) ;
        add(history);
        add(delta);
        addCounters() ;
    }
    
    private void add(ServiceRef srvRef) {
        getServiceRefs().add(srvRef) ;
        for ( String ep : srvRef.endpoints )
            endpoints.put(ep, srvRef) ;
    }

    public ServiceRef getServiceRef(String service) {
        if ( ! initialized )
            Fuseki.serverLog.error("Not initialized: dataset = "+name) ;
        if ( service.startsWith("/") )
            service = service.substring(1, service.length()) ; 
        return endpoints.get(service) ;
    }
    
    private void addCounters() {
        getCounters().add(CounterName.Requests) ;
        getCounters().add(CounterName.RequestsGood) ;
        getCounters().add(CounterName.RequestsBad) ;

        query.getCounters().add(CounterName.Requests) ;
        query.getCounters().add(CounterName.RequestsGood) ;
        query.getCounters().add(CounterName.RequestsBad) ;
        query.getCounters().add(CounterName.QueryTimeouts) ;
        query.getCounters().add(CounterName.QueryExecErrors) ;

        update.getCounters().add(CounterName.Requests) ;
        update.getCounters().add(CounterName.RequestsGood) ;
        update.getCounters().add(CounterName.RequestsBad) ;
        update.getCounters().add(CounterName.UpdateExecErrors) ;

        upload.getCounters().add(CounterName.Requests) ;
        upload.getCounters().add(CounterName.RequestsGood) ;
        upload.getCounters().add(CounterName.RequestsBad) ;

        history.getCounters().add(CounterName.Requests) ;
        history.getCounters().add(CounterName.RequestsGood) ;
        history.getCounters().add(CounterName.RequestsBad) ;

        delta.getCounters().add(CounterName.Requests) ;
        delta.getCounters().add(CounterName.RequestsGood) ;
        delta.getCounters().add(CounterName.RequestsBad) ;
        
        addCountersForGSP(readWriteGraphStore.getCounters(), false) ;
        if ( readGraphStore != readWriteGraphStore )
            addCountersForGSP(readGraphStore.getCounters(), true) ;
    }

    private void addCountersForGSP(CounterSet cs, boolean readWrite) {
        cs.add(CounterName.Requests) ;
        cs.add(CounterName.RequestsGood) ;
        cs.add(CounterName.RequestsBad) ;

        cs.add(CounterName.GSPget) ;
        cs.add(CounterName.GSPgetGood) ;
        cs.add(CounterName.GSPgetBad) ;

        cs.add(CounterName.GSPhead) ;
        cs.add(CounterName.GSPheadGood) ;
        cs.add(CounterName.GSPheadBad) ;

        // Add anyway.
        // if ( ! readWrite )
        // return ;

        cs.add(CounterName.GSPput) ;
        cs.add(CounterName.GSPputGood) ;
        cs.add(CounterName.GSPputBad) ;

        cs.add(CounterName.GSPpost) ;
        cs.add(CounterName.GSPpostGood) ;
        cs.add(CounterName.GSPpostBad) ;

        cs.add(CounterName.GSPdelete) ;
        cs.add(CounterName.GSPdeleteGood) ;
        cs.add(CounterName.GSPdeleteBad) ;

        cs.add(CounterName.GSPpatch) ;
        cs.add(CounterName.GSPpatchGood) ;
        cs.add(CounterName.GSPpatchBad) ;

        cs.add(CounterName.GSPoptions) ;
        cs.add(CounterName.GSPoptionsGood) ;
        cs.add(CounterName.GSPoptionsBad) ;
    }
}
