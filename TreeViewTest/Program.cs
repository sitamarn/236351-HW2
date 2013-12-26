using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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
               first.MachineDroppedHandler = delegate(List<String> prim, List<String> bup)
               {
                   Console.WriteLine("Machine dropped handler");
                   Console.WriteLine("Sellers who lost primary: " + String.Join("\n", prim));
                   Console.WriteLine("Sellers who lost backup: " + String.Join("\n", bup));
               };
               first.MachineJoinedHandler = delegate(String originalSeller, Uri uri)
               {
                   Console.WriteLine("Machine joined handler");
                   Console.WriteLine("Original seller joined: "  + originalSeller);
                   Console.WriteLine("New machine URI: " + uri);
               };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something failed...");
                Console.WriteLine(ex.Message);
            }

            Thread.Sleep(1000);

            Console.WriteLine("Adding Node...");
            ZooKeeperWrapper zk = new ZooKeeperWrapper("localhost", 10000, 5, null);
            ZNodesDataStructures.MachineNode data = new ZNodesDataStructures.MachineNode();
            data.originalSellerName = "nigger"; 
            data.uri = new Uri("http://blackstreet:1024");
            data.primaryOf = new List<string>();
            data.primaryOf.Add("nigger");

            string newId = zk.Create(first.MachinesPath + "/crap", 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EphemeralSequential
                );

            Console.WriteLine("Created ephermal " + newId);
            Thread.Sleep(1000);

            Console.WriteLine("Press return to continue");
            Console.ReadLine();

            Console.WriteLine("Removing Node...");
            zk.Delete(newId);
            Console.WriteLine("Node deleted :-(");
            Console.WriteLine("Press return to continue");
            Console.ReadLine();

            first.Dispose();
            Console.WriteLine("Press any key to quit");
            Console.ReadLine();
        }
    }
}
