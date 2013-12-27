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
            ReplicationDriver rd = new ReplicationDriver("myCluster");
            rd.purgeZooKeeper();

            try
            {
                first = new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "myFirstSeller", new Uri("http://localhost:123"));
                second= new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "mySecondSeller", new Uri("http://remoteHost:456"));

                MachineDropped droppedHandler = delegate(List<String> prim, List<String> bup)
                {
                    Console.WriteLine("Machine dropped handler");
                    Console.WriteLine("Sellers who lost primary: " + String.Join("\n", prim));
                    Console.WriteLine("Sellers who lost backup: " + String.Join("\n", bup));
                };
                
                MachineJoined joinedHandler = delegate(String machineName, String originalSeller, Uri uri)
                {
                    Console.WriteLine("Machine "+machineName+" joined handler");
                    Console.WriteLine("Original seller joined: "  + originalSeller);
                    Console.WriteLine("New machine URI: " + uri);
                };

                first.MachineDroppedHandler =  droppedHandler;
                first.MachineJoinedHandler = joinedHandler;
                second.MachineDroppedHandler = droppedHandler;
                second.MachineJoinedHandler = joinedHandler;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something failed...");
                Console.WriteLine(ex.Message);
            }

            Thread.Sleep(1000);

            
            rd.addSeller("Kushim");
            Thread.Sleep(1000);
            Console.WriteLine(first.ToString());
            Console.WriteLine("* Added seller");
            Console.ReadKey();

            ZNodesDataStructures.MachineNode mn = new ZNodesDataStructures.MachineNode()
            {
                primaryOf = new List<String>() { "Kushim" },
                uri = new Uri("http://localhost:123"),
                backsUp = new List<String>(),
                originalSellerName = "myFirstSeller"
            };

            Console.WriteLine("Updating first seller to be a primary of someone");
            first.updateMachineData(mn);

            Thread.Sleep(1000);
            Console.WriteLine(first.ToString());
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();

            mn.primaryOf.Clear();

            first.updateMachineData(mn);

            Thread.Sleep(1000);
            Console.WriteLine(first.ToString());
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }
    }
}
