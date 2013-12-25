using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZooKeeperNet;

namespace TreeViewLib
{
    public class ZooKeeperWrapper
    {
        private Boolean verbose = false;
        private readonly int retries;
        private readonly int timeout;
        private IWatcher treeWatcher = null;
        private string zookeeperAddress = null;
        private ZooKeeper zk = null;
        public ZooKeeper Handler { get { return zk; } }

        public ZooKeeperWrapper(String address, int timeout_sec, int retry, IWatcher watcher, Boolean verbose = false)
        {
            retries = retry;
            timeout = timeout_sec;
            treeWatcher = watcher;
            zookeeperAddress = address;
            this.verbose = verbose;
            Connect();
        }

        private void Connect()
        {
            if (zk != null)
            {
                try
                {
                    zk.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Disposal of old Zookeeper failed, I still gonna kick the shit out of him");
                    Console.WriteLine("Error message: " + ex.Message);
                }
                zk = null;
            }
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    zk = new ZooKeeper(zookeeperAddress, new TimeSpan(0, 0, timeout), treeWatcher);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Creating new zookeeper exception after #" + retries + "retrues :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
                if (zk != null)
                {
                    break;
                }
            }
        }

        public void Delete(String path)
        {
            if (Exists(path))
            {
                Stat s = GetStat(path);
                if(s!=null) {
                    Delete(path, s.Version);
                }
            }
        }

        public void Delete(String path, int version)
        {
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    zk.Delete(path, version);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Delete exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }

            }           
        }

        public List<String> GetChildren(string path, IWatcher watcher)
        {
            checkRep();
            List<String> lst = null;
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    var children = zk.GetChildren(path, watcher);
                    if (children != null)
                    {
                        lst = children.ToList();
                    }
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("GetChildren exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }

            }

            return lst;
        }

        public List<String> GetChildren(string path, Boolean watch)
        {
            checkRep();
            List<String> lst = new List<String>();
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    var children = zk.GetChildren(path, watch);
                    if (children != null)
                    {
                        lst.AddRange(children.ToList());
                    }
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("GetChildren exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }

            }

            return lst;
        }

        public T GetData<T>(string nodePath, IWatcher watcher) where T : new()
        {
            T data = default(T);
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    byte[] readData = zk.GetData(nodePath, watcher, null);
                    data = ZNodesDataStructures.deserialize<T>(readData);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("GetData exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
            return data;
        }

        public T GetData<T>(string nodePath, Boolean watch) where T : new()
        {
            T data = default(T);
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    byte[] readData = zk.GetData(nodePath, watch, null);
                    data = ZNodesDataStructures.deserialize<T>(readData);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("GetData exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
            return data;
        }

        public Stat SetData<T>(string nodePath, T data, int version) where T : new()
        {
            Stat stat = null;
            byte[] serializedData = ZNodesDataStructures.serialize<T>(data);
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    stat = zk.SetData(nodePath, serializedData, version);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("SetData exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
            return stat;
        }

        public void Register(IWatcher watcher)
        {
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    zk.Register(watcher);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Register exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
        }

        public String Create<T>(string path, T data, IEnumerable<Org.Apache.Zookeeper.Data.ACL> acl, CreateMode mode)
        {
            return Create(path, ZNodesDataStructures.serialize(data), acl, mode);
        }

        public String Create(string path, byte[] data, IEnumerable<Org.Apache.Zookeeper.Data.ACL> acl, CreateMode mode)
        {
            String name = null;
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    name = zk.Create(path, data, acl, mode);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Create exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
            return name;
        }

        public Stat GetStat(string path)
        {
            Stat stat = null;
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    stat = zk.Exists(path, false);
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Exists exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }
            return stat;
        }


        public Boolean Exists(string path)
        {
            Boolean exists = false;
            int tries = retries;
            while ((tries--) > 0)
            {
                try
                {
                    Stat s = zk.Exists(path, false);
                    if (s != null)
                    {
                        exists = true;
                    }
                    break;
                }
                catch (Exception ex)
                {
                    if (tries == 0)
                    {
                        Console.WriteLine("Exists exception after #" + retries + " retries :\n" + ex.Message);
                        Console.WriteLine("Last retry, throwing exception");
                        throw ex;
                    }
                }
            }

            return exists;
        }

        private void checkRep()
        {
            if (zk == null)
            {
                throw new Exception("Zookeeper is unintialized :(");
            }
            if (retries < 1)
            {
                throw new Exception(" retries should be >= 1");
            }
        } 

    }
}
