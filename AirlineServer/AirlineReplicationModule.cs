using FlightSearchServer;
using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace AirlineServer
{
    class AirlineReplicationModule : IAirlineReplicationModule
    {
        static ZooKeeper zk = null;
        static readonly string MACHINES_NODE = "machines";
        static readonly string SELLERS_NODE = "sellers";
        static readonly int SECONDS_TIMEOUT = 10000;

        private string clusterName;
        private string zookeeperAddress;

        private string id = null;

        private ZooKeeperEvent machineNodeWatch = null;

        public class ZooKeeperEvent : IWatcher
        {
            AirlineReplicationModule replicationModule = null;

            public void Process(WatchedEvent @event)
            {
                replicationModule.handleMachineNodeEvent(@event);
            }

            public ZooKeeperEvent(AirlineReplicationModule repMod)
            {
                replicationModule = repMod;
            }
        }

        static byte[] serialize<T>(T sz) 
        {
            if (sz == null) { return null; }
            byte[] content = null;
            try
            {
                IFormatter bf = new BinaryFormatter();
                using (MemoryStream ms = new MemoryStream())
                {
                    using (var ds = new DeflateStream(ms, CompressionMode.Compress, true))
                    {
                        bf.Serialize(ds, sz);
                    }
                    ms.Position = 0;
                    content = ms.GetBuffer();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Serialization process failed - " + ex.Message);
            }
            return content;
        }

        static T deserialize<T>(byte[] sz) where T: new()
        {
            if (sz == null) { return default(T); }
            var nodeData = new T();
            try
            {
                var formatter = new BinaryFormatter();
                using (var ms = new MemoryStream(sz))
                {
                    using (var ds = new DeflateStream(ms, CompressionMode.Decompress, true))
                    {
                        nodeData = (T)formatter.Deserialize(ds);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deserializing machine - " + ex.Message);
            }

            return nodeData;
        }

        [Serializable]
        public class MachineNode
        {
            public List<String> primaryOf = new List<String>();
            public List<String> backsUp = new List<String>();
        }

        public AirlineReplicationModule(String address, String clusterName, String originalSeller)
        {
            this.clusterName = clusterName;
            this.zookeeperAddress = address;

            if (null == zk)
            {
                //zk = new ZooKeeper( address, 
                //    new TimeSpan(0,0,SECONDS_TIMEOUT), 
                //    new ZooKeeperEvent()
                //    ); 
            }

            checkCluster(); // Create cluster node if needed
            checkMachinesNode(); // Create machines node if needed
            checkSellersNode(); // Create Sellers node if needed
            registerMachinesNode(); // Add this to Machines subtree
            setWatches();
        }

        private void setWatches()
        {
            // 1. Watch and check if my data changes
            //machineNodeWatch = new ZooKeeperEvent();
            
            //zk.
        }

        private void checkMachinesNode()
        {
            try
            {
                Stat s = zk.Exists("/" + clusterName + "/" + MACHINES_NODE, false);
                if (null == s)
                {
                    zk.Create("/" + clusterName + "/" + MACHINES_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
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
                Stat s = zk.Exists("/" + clusterName + "/" + SELLERS_NODE, false);
                if (null == s)
                {
                    zk.Create("/" + clusterName + "/" + SELLERS_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
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
        public void registerMachinesNode()
        {
            try
            {
                var node = new MachineNode();
                byte[] serializedNode = serialize(node);
                id = zk.Create("/" + clusterName + "/" + MACHINES_NODE, serializedNode , Ids.OPEN_ACL_UNSAFE, CreateMode.EphemeralSequential);
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Couldn't reach Zookeeper server, " + e.Message);
                throw e;
            }            
        }

        /// <summary>
        /// Jumps out of the ephemeral node
        /// </summary>
        public void primaryMachineDown()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Watch my own data changed 
        /// </summary>
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

        public bool isLeader()
        {
            if (id == null) return false;
            return (getLeaderId()==id);
        }

        public string getLeaderId()
        {
            string machinesPath = clusterName + "/" + MACHINES_NODE;
            IEnumerable<String> children = zk.GetChildren(machinesPath, false);
            return children.Min();
        }

        /// <summary>
        /// Handles 
        /// </summary>
        /// <param name="event"></param>
        private void handleMachineNodeEvent(WatchedEvent @event)
        {
            //if(@event==
        }
    }
}
