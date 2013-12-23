using FlightSearchServer;
using Org.Apache.Zookeeper.Data;
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
        static readonly string MACHINES_NODE = "/machines";
        static readonly string SELLERS_NODE = "/sellers";
        static readonly int SECONDS_TIMEOUT = 10000;

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
                zk = new ZooKeeper( address, 
                    new TimeSpan(0,0,SECONDS_TIMEOUT), new ZooKeeperEvent()); 
            }

            checkCluster();
            checkMachinesNode();
            checkSellersNode();

        }

        private void checkMachinesNode()
        {
            try
            {
                Stat s = zk.Exists("/" + clusterName + MACHINES_NODE, false);
                if (null == s)
                {
                    zk.Create("/" + clusterName + MACHINES_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Couldn't reach Zookeeper server, " + e.Message);
                throw e;
            }
        }

        private void checkSellersNode()
        {
            try
            {
                Stat s = zk.Exists("/" + clusterName + MACHINES_NODE, false);
                if (null == s)
                {
                    zk.Create("/" + clusterName + MACHINES_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Couldn't reach Zookeeper server, " + e.Message);
                throw e;
            }
        }


        private void checkCluster()
        {
            try
            {
                Stat s = zk.Exists("/" + clusterName, false);
                if (null == s)
                {
                    zk.Create("/" + clusterName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Couldn't reach Zookeeper server, " + e.Message);
                throw e;
            }
        }

        /// <summary>
        /// Initialize this as a new machine - this method will add this to the zookeeper tree
        /// </summary>
        public void initNewMachine()
        {
            // TODO we need to create a znode of Ephemeral.Sequential
            try
            {
                Stat s = zk.Exists("/" + clusterName + MACHINES_NODE + "/" + , false);
                if (null == s)
                {
                    zk.Create("/" + clusterName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Couldn't reach Zookeeper server, " + e.Message);
                throw e;
            }            
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


        public Dictionary<string, Uri> getSellersAndPrimaries()
        {
            throw new NotImplementedException();
        }
    }
}
