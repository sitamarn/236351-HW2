using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using ZooKeeperNet;

namespace TreeViewLib
{
    public class AirlineReplicationModule : IAirlineReplicationModule
    {
        //static ZooKeeper zk = null;
        private ZooKeeperWrapper zk = null;

        public static readonly string MACHINE_PREFIX = "machine_";
        public static readonly string MACHINES_NODE = "machines";
        public static readonly string SELLERS_NODE = "sellers";
        public static readonly int SECONDS_TIMEOUT = 10000;
        public static readonly int ZK_RETRIES = 5;

        private string clusterName;
        private string zookeeperAddress;

        private string id = null;
        TreeViewLib.TreeView treeView = new TreeView();

        public string SellersPath { get { return "/" + clusterName + "/" + SELLERS_NODE; } }
        public string MachinesPath { get { return "/" + clusterName + "/" + MACHINES_NODE; } }
        public string MachinePath { get { return id;  } }

        public abstract class AirlineReplicationModuleWatcher : IWatcher {
            protected AirlineReplicationModule replicationModule = null;

            public AirlineReplicationModuleWatcher(AirlineReplicationModule repMod)
            {
                replicationModule = repMod;
            }

            public void Process(WatchedEvent @event) { }
        }

        public class ZooKeeperTreeEvent : AirlineReplicationModuleWatcher
        {
            public ZooKeeperTreeEvent(AirlineReplicationModule airlineReplicationModule) : base(airlineReplicationModule) {} 

            public void Process(WatchedEvent @event)
            {
                replicationModule.treeNodeEvent(@event);
            }            
        }

        public class MachineDataWatch : AirlineReplicationModuleWatcher
        {
            public MachineDataWatch(AirlineReplicationModule mod) : base(mod) { }

            public void Process(WatchedEvent @event)
            {
                replicationModule.machineNodeDataChanged(@event);
            }
        }

        public class MachinesNodeWatch : AirlineReplicationModuleWatcher
        {
            public MachinesNodeWatch(AirlineReplicationModule mod) : base(mod) { }

            public void Process(WatchedEvent @event)
            {
                replicationModule.machinesNodeEvent(@event);
            }
        }

        public class SellersNodeWatch : AirlineReplicationModuleWatcher
        {
            public SellersNodeWatch(AirlineReplicationModule mod) : base(mod) { }

            public void Process(WatchedEvent @event)
            {
                replicationModule.sellersNodeEvent(@event);
            }
        }

        public AirlineReplicationModule(String address, String clusterName, String originalSeller)
        {
            this.clusterName = clusterName;
            this.zookeeperAddress = address;

            if (null == zk)
            {
                zk = new ZooKeeperWrapper(address,SECONDS_TIMEOUT, ZK_RETRIES, new ZooKeeperTreeEvent(this));
            }

            constructClusterSubTree(); // Constructs the cluster's subtree if needed
            registerMachinesNode();

            setMachinesChildrenWatcher();
            setMachineNodeDataWatcher();
            setSellersChildrenWatcher();
            // Iterate tree and start building the view
            buildTreeView();

            // How to deal with leader events ?
        }


        /// <summary>
        /// This function iterates the zookeeper tree and generates a local view of it in the TreeView class
        /// </summary>
        private void buildTreeView()
        {
 	        //throw new NotImplementedException();
        }

        /// <summary>
        /// Sets a watcher on the Sellers node, event will call the children changed evend
        /// </summary>
        private void setSellersChildrenWatcher()
        {
            try
            {
                zk.GetChildren(SellersPath, new SellersNodeWatch(this)); // Register creation/deletion of children
            }  catch(Exception ex) 
            {
                Console.WriteLine("Failed setting Sellers Node watch due to " + ex.Message);
                throw ex;
            }
        }

        /// <summary>
        /// Sets a watcher on the machine's ephemeral node, this will call the ..... function
        /// </summary>
        private void setMachineNodeDataWatcher()
        {
            try
            {
                zk.GetData<ZNodesDataStructures.MachineNode>(MachinePath, new MachineDataWatch(this));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed setting Machine data Node watch due to " + ex.Message);
                throw ex;
            }           
        }

        private void setMachinesChildrenWatcher()
        {
            try
            {
                zk.GetChildren(MachinesPath, new MachinesNodeWatch(this));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed setting Machines Node children watch due to " + ex.Message);
                throw ex;
            }                       
        }

        private void setTreeNodeEvent()
        {
            try
            {
                zk.Register(new ZooKeeperTreeEvent(this));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Couldn't set tree watcher because: " + ex.Message);
                throw ex;
            }
        }

        /// <summary>
        /// this function will construct this cluster's subtree if it doesn't exist. cluster's subtree
        /// consists of the parent [cluster] and the children [sellers, nodes].
        /// </summary>
        private void constructClusterSubTree()
        {
            // Construct the cluster's root if it doesn't exist
            try
            {
                Boolean exists = zk.Exists("/" + clusterName);
                if (!exists)
                {
                    zk.Create("/" + clusterName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName  + " failed\n" + e.Message);
                throw e;
            }
            // Construct the child MACHINES_NODE if needed
            try
            {
                Boolean exists = zk.Exists("/" + clusterName + "/" + MACHINES_NODE);
                if (!exists)
                {
                    zk.Create("/" + clusterName + "/" + MACHINES_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName + "/" + MACHINES_NODE + " failed\n"+e.Message);
                throw e;
            }
            // Construct the child SELLERS_NODE if needed
            try
            {
                Boolean exists = zk.Exists("/" + clusterName + "/" + SELLERS_NODE);
                if (!exists)
                {
                    zk.Create("/" + clusterName + "/" + SELLERS_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName + "/" + SELLERS_NODE + " failed\n" + e.Message);
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
                var node = new ZNodesDataStructures.MachineNode();
                byte[] serializedNode = ZNodesDataStructures.serialize(node);
                id = zk.Create(MachinesPath + "/" + MACHINE_PREFIX, serializedNode , Ids.OPEN_ACL_UNSAFE, CreateMode.EphemeralSequential);
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Registering machine node failed\n" + e.Message);
                throw e;
            }            
        }

        public Dictionary<string, Uri> getSellersAndPrimaries()
        {
            throw new NotImplementedException();
        }

        internal void treeNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("treeNodeEvent event: " + @event.Type + " on " + @event.Path);

            setTreeNodeEvent();
        }

        internal void machinesNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("machinesNodeEvent event: " + @event.Type + " on " + @event.Path);
            string[] path = @event.Path.Split('/');
            if ((path.Length >= 2) && (path[0].Equals(clusterName) && (path[1].Equals(MACHINES_NODE))))
            {
                switch (@event.Type)
                {
                    case EventType.NodeCreated:  // New child created, get it's data and put it inside my TreeView
                        //zk.GetChildren(
                        break;
                    case EventType.NodeDeleted:
                        // Check if its the MACHINES_NODE, if it is, throw an exception, otherwise update the tree
                        break;
                }
            }
            else
            {
                Console.WriteLine("Got message on path " + @event.Path + " Which doesn't belong to this TreeView, ignoring");
            }


            setMachinesChildrenWatcher();
        }

        internal void machineNodeDataChanged(WatchedEvent @event)
        {
            Console.WriteLine("machineNodeDataChanged event: " + @event.Type + " on " + @event.Path);
            if (@event.Type == EventType.NodeDataChanged)
            {
                // 1. Get path node
                // 2. Update the data on the treeView
                string[] path = @event.Path.Split('/');
                // Check if its our cluster / MACHINES_NODE / <someID>
                if ((path.Length == 3) && path[0].Equals(clusterName) && path[1].Equals(MACHINES_NODE) && @event.Path.Equals(id))
                {
                    string machineNode = path[2];
                    var data = zk.GetData<ZNodesDataStructures.MachineNode>(@event.Path, false);
                    treeView.setMachineData(machineNode, data);
                    
                }
                else
                {
                    Console.WriteLine("Path " + @event.Path + " is not mine ["+id+"], ignoring event");
                }
                
                
            }
            setMachineNodeDataWatcher();
        }

        internal void sellersNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("sellersNodeEvent event: " + @event.Type + " on " + @event.Path);
            setSellersChildrenWatcher();
        }


    }
}
