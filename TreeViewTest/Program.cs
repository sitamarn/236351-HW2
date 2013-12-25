using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TreeViewLib;
using ZooKeeperNet;
namespace TreeViewTest
{
    class Program
    {
        static void Main(string[] args)
        {
            TreeViewLib.AirlineReplicationModule first = null;
            TreeViewLib.AirlineReplicationModule second = null;
            try
            {
               first = new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "myFirstSeller", new Uri("http://localhost:123"));
               //second = new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "mySecondSeller", new Uri("http://localhost:456"));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something failed...");
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Adding Node...");
            ZooKeeperWrapper zk = new ZooKeeperWrapper("localhost", 10000, 5, null);
            string newId = zk.Create(first.MachinesPath + "/crap", 
                ZNodesDataStructures.serialize(new ZNodesDataStructures.MachineNode()), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EphemeralSequential
                );
            
            Console.WriteLine("Created ephermal " + newId);
            Console.WriteLine("Press return to continue");
            Console.ReadLine();

            Console.WriteLine("Removing Node...");
            zk.Delete(newId);
            Console.WriteLine("Node deleted :-(");
            Console.WriteLine("Press return to continue");
            Console.ReadLine();
            

            if (zk == null)
            {
                Console.WriteLine("Connection died too many times :-(, qutting");
                return;
            }

            foreach (var child in zk.GetChildren("/",null)) {
                Console.WriteLine("Root children: " + child);
                foreach (var secondChild in zk.GetChildren("/"+child, false))
                {
                    Console.WriteLine("Sub trees: " + secondChild);
                }
            }

            Console.WriteLine("Machines in system: ");
            foreach (var item in zk.GetChildren(first.MachinesPath, false).ToList())
            {
                Console.WriteLine(item);
            }

            //Console.WriteLine("Checking tree view: ");
            //foreach (var machine in second.Tree.Machines)
            //{
            //    Console.WriteLine("Name: " + machine.Key + " Uri: " + machine.Value.uri);
            //}

            Console.WriteLine("Press any key to quit");
            Console.ReadLine();
        }
    }
}
