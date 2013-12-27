using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;


namespace TreeViewLib
{
    interface IAirlineReplicationModule
    {

        void initialize(String address, String clusterName, String originalSeller, Uri localService);
        void updateMachineData(ZNodesDataStructures.MachineNode machineData);
        

        /// <summary>
        /// This function is called when a machine drops from the Zookeeper tree
        /// </summary>
//        void primaryMachineDown();
        /// <summary>
        /// Data of this machine's node changed
        /// </summary>
//        void replicationRequest();
        /// <summary>
        /// Call back when a new machine joins the cluster
        /// </summary>
//        void newMachineJoined();

        /// <summary>
        /// Query sellers and their primaries
        /// </summary>
        /// <returns></returns>
        Dictionary<string, Uri> getSellersAndPrimaries();

        /// <summary>
        /// Singleton entry point to the airline replication module
        /// </summary>
        /// <returns></returns>
        //static IAirlineReplicationModule getAirlineReplicationModule();

        
    }
}
