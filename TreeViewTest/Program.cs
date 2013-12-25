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
               first = new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "myFirstSeller");
               second = new TreeViewLib.AirlineReplicationModule("localhost", "myCluster", "mySecondSeller");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Something failed...");
                Console.WriteLine(ex.Message);
            }

            ZooKeeperWrapper zk = new ZooKeeperWrapper("localhost", 10000, 5, null);

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
        }
    }
}
