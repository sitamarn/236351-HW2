using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;


namespace FlightSearchServer
{
    class IAirlineReplicationModule
    {
        public interface IZooKeeperEvent : IWatcher
        {
            public IZooKeeperEvent(String node);
        }

        /// <summary>
        /// Initialize internal state
        /// </summary>
        public IAirlineReplicationModule(); 
        /// <summary>
        /// Init this machine as new
        /// </summary>
        public void initNewMachine();
        /// <summary>
        /// This function is called when a machine drops from the Zookeeper tree
        /// </summary>
        public void primaryMachineDown();
        /// <summary>
        /// Data of this machine's node changed
        /// </summary>
        public void replicationRequest();
        /// <summary>
        /// Call back when a new machine joins the cluster
        /// </summary>
        public void newMachineJoined();
    }
}
