using FlightSearchServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace AirlineServer
{
    class AirlineReplicationModule : IAirlineReplicationModule
    {
        static ZooKeeper zk = null;
        static string MachinesNode = "/machines";
        static string SellersNode = "/sellers";
        static int secondsTimeout = 10000;

        private string clusterName;
        private string zookeeperAddress;

        public class ZooKeeperEvent : IAirlineReplicationModule.IZooKeeperEvent
        {
            public void Process(ZooKeeperNet.WatchedEvent @event)
            {
                throw new NotImplementedException();
            }
        }

        public AirlineReplicationModule(String address, String clusterName, String originalSeller)
        {
            this.clusterName = clusterName;
            this.zookeeperAddress = address;

            if (null == zk)
            {
                //zk = new ZooKeeper( address, 
                //    new TimeSpan(0,0,secondsTimeout), new IWatcher() {
                    
                //});
            }
        }

        /// <summary>
        /// Initialize this as a new machine - this method will add this to the zookeeper tree
        /// </summary>
        public void initNewMachine()
        {
            throw new NotImplementedException();
        }

        public void primaryMachineDown()
        {
            throw new NotImplementedException();
        }

        public void replicationRequest()
        {
            throw new NotImplementedException();
        }

        public void newMachineJoined()
        {
            throw new NotImplementedException();
        }
    }
}
