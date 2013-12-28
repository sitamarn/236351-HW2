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
        public static void doBarrierStuff(AirlineReplicationModule repMod, String id) 
        {
            Random random = new Random();
            int secondsToWait = random.Next(0, 2);
            Console.WriteLine("[" + id + "] starting to do some job - " + secondsToWait + " sec");
            Thread.Sleep(secondsToWait * 1000);
            Console.WriteLine("[" + id + "] finished doing some job, Going to barrier....");
            repMod.barrier();
            Console.WriteLine("["+ id + "] Barrier finished");
        }



        static void Main(string[] args)
        {
            TreeViewLib.AirlineReplicationModule first = null;
            TreeViewLib.AirlineReplicationModule second = null;
            ReplicationDriver rd = new ReplicationDriver("myCluster");
            rd.purgeZooKeeper();
            MachineJoined joinedHandler = null;
            MachineJoined joinedHandlerSecond = null;
            MachineJoined joinedHandlerThird = null;
            MachineDropped droppedHandler = null;
            try
            {
                first = new TreeViewLib.AirlineReplicationModule();

                droppedHandler = delegate(List<String> prim, List<String> bup)
                {
                    doBarrierStuff(first, "FIRST DROP");
                };
                
                joinedHandler = delegate(String machineName, String originalSeller, Uri uri)
                {
                    doBarrierStuff(first, "FIRST JOIN");
                };

                first.MachineDroppedHandler =  droppedHandler;
                first.MachineJoinedHandler = joinedHandler;
            
                first.LoadBalancingDoneHandler = delegate()
                {
                    Console.WriteLine("First Balancing done");
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something failed...");
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Initializing first object");
            first.initialize("localhost", "myCluster", "myFirstSeller", new Uri("http://localhost:123"));
            Thread.Sleep(5000);

            second = new TreeViewLib.AirlineReplicationModule();
            second.MachineDroppedHandler = delegate(List<String> prim, List<String> bup) 
            {
                doBarrierStuff(second, "SECOND DROP");
            };
            second.MachineJoinedHandler = delegate(String machineName, String originalSeller, Uri uri)
                                {
                                    doBarrierStuff(second, "SECOND JOIN");
                                };
            Console.WriteLine("Adding second machine");
            second.initialize("localhost", "myCluster", "mySecondSeller", new Uri("http://remoteHost:456"));

            Thread.Sleep(6000);

            ZNodesDataStructures.MachineNode newMachineNode = new ZNodesDataStructures.MachineNode()
            {
                primaryOf = new List<String>() ,
                uri = new Uri("http://new_machine_seller:123"),
                backsUp = new List<String>(),
                originalSellerName = "NEW_MACHINE_SELLER"
            };

            //Console.WriteLine("====================== Adding NEW_MACHINE_ADDED ===========================");

            Console.WriteLine("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
            TreeViewLib.AirlineReplicationModule third = new AirlineReplicationModule();
            third.MachineJoinedHandler = delegate(String machineName, String originalSeller, Uri uri)
            {
                doBarrierStuff(third, "THIRD JOIN");
            };
            third.MachineDroppedHandler = delegate(List<String> j, List<String> l)
            {
                doBarrierStuff(third, "THIRD DROP");
            };

            third.initialize("localhost", "myCluster", "NiggerSeller", new Uri("http://localhost:666"));
            //rd.addMachine("NEW_MACHINE_ADDED", newMachineNode);
            Console.WriteLine("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
            //Console.WriteLine("=============SHOWING TREE===================");
            //Console.WriteLine(first.ToString());
            //Console.WriteLine("* Added machine");

            Console.WriteLine("PRESS ANY KEY TO CONTINUE & KILL FIRST");
            Console.ReadKey();
            Console.WriteLine("-=-=-=-=-=-=-=- Killing an instance -=-=-=-=-=-=-=-=-=-=-");
            first.testDie();
            Thread.Sleep(5000);
            Console.WriteLine("PRESS ANY KEY TO CONTINUE & KILL FIRST");
            Console.ReadKey();
            
            ////Thread.Sleep(7000);
            //Console.WriteLine("Press any key to continue");
            //Console.ReadKey();


            rd.addSeller("Kushim");
            Thread.Sleep(1000);
            Console.WriteLine(second.ToString());
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
            second.updateMachineData(mn);

            Thread.Sleep(1000);
            Console.WriteLine(second.ToString());
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();

            mn.primaryOf.Clear();

            second.updateMachineData(mn);

            Thread.Sleep(1000);
            Console.WriteLine(second.ToString());
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }
    }
}
