using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;
using Registeration;
using System.Net;
using TreeViewLib;
using System.Configuration;

namespace AirlineServer
{
    class Program
    {
        
        /// <summary>
        /// Read the seller's flight from the given file
        /// </summary>
        static AirlineServer.Seller readSellerFile(string filePath, string sellerName)
        {
            List<AirlineServer.Flight> flights = new List<Flight>();
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(filePath);
                string line = reader.ReadLine();
                while (line != null)
                {
                    AirlineServer.Flight flight = new Flight();
                    string[] tokens = line.Split(' ');
                    flight.flightNumber = tokens[0];
                    flight.src = tokens[1];
                    flight.dst = tokens[2];
                    flight.seller = sellerName;
                    flight.date = DateTime.Parse(tokens[3]);
                    flight.price = Convert.ToInt32(tokens[4]);
                    flights.Add(flight);
                    line = reader.ReadLine();
                }

            }
            catch
            {
                throw;
            }
            finally { 
                reader.Close(); 
            }

            AirlineServer.Seller seller = new Seller();
            seller.name = sellerName;
            seller.flights = flights;
            return seller;
        }

        static string readZooKeeperConfiguration(string filePath)
        {
            StreamReader reader = null;
            string line = null;
            try
            {
                reader = new StreamReader(filePath);
                line = reader.ReadLine();
            }
            catch
            {
                throw;
            }
            finally
            {
                reader.Close();
            }

            return line;
        }

        static string readZooKeeperAddress()
        {
            AppSettingsReader cnfg = new AppSettingsReader();
            string zkAddress = (string)cnfg.GetValue("ZooKeeperCNFG", typeof(string));

            return readZooKeeperConfiguration(zkAddress);
        }

        /// <summary>
        /// Main :)
        /// </summary>
        /// <param name="args"><name> <alliance> <search server port> <airline servers port> <flights search server URI #2> <input file></param>
        static void Main(string[] args)
        {
            CacheData cache = new CacheData();
            if (args.Length != 6)
            {
                Console.WriteLine("Bad arguments");
                Console.WriteLine("TicketSellingServer.exe <name> <alliance> <search server port> <airline servers port> <flights search server URI #2> <input file>");
                return;
            }

            string url = null;
            AirlineServer.Seller seller = null;

            // Check the input:
            try
            {
                url = @"http://" + args[4];
                Convert.ToInt32(args[2]);
                Convert.ToInt32(args[3]);
                new Uri(url);
                seller = readSellerFile(args[5], args[0]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Bad arguments: " + e.Message);
                return;
            }

            string zkAddress = null;
            try
            {
                zkAddress = readZooKeeperAddress();
            }
            catch
            {
                Console.WriteLine("Error: bad configuration file!");
                return;
            }

            // TODO: remove this
            String ip = "localhost";
            if (args[5].StartsWith(@"c:\"))
            {
                ip = "192.168.0.1";
            }

            // Read arguments
            string intraClusterAddress = @"http://"+ip+":" + args[3] + @"/IntraClusterService";
            string sellerAddress = @"http://"+ip+":" + args[2] + @"/SellerService";

           // Builder
            IntraClusterService ics = new IntraClusterService(cache, seller, url, sellerAddress, args[1]);
            AirlineReplicationModule.Instance.initialize(zkAddress, args[1], args[0], new Uri(intraClusterAddress), ics.respondIfNewNode, ics.respondIfSomeoneLeft);
            SellerService sa = new SellerService();
            
            AirlineReplicationModule.Instance.waitForNameRegister();
            ics.setName(AirlineReplicationModule.Instance.MachineName);
            try
            {
                using (ServiceHost sellerHost = new ServiceHost(sa, new Uri(sellerAddress)))
                {
                    using (ServiceHost intraHost = new ServiceHost(ics, new Uri(intraClusterAddress)))
                    {


                        ServiceEndpoint sellerEndPoint = sellerHost.AddServiceEndpoint(typeof(ISellerService), new BasicHttpBinding(), sellerAddress);
                        ServiceEndpoint intraEndPoint = intraHost.AddServiceEndpoint(typeof(ISellerClusterService), new BasicHttpBinding(), intraClusterAddress);
                        
                        // add http get support
                        ServiceMetadataBehavior Ismb = new ServiceMetadataBehavior();
                        Ismb.HttpGetEnabled = true;
                        sellerHost.Description.Behaviors.Add(Ismb);
                        sellerHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode = InstanceContextMode.Single;
                        sellerHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().IncludeExceptionDetailInFaults = true;
                        intraHost.Description.Behaviors.Add(Ismb);

                        ExOpBehavior cacheBehavior = new ExOpBehavior(cache);
                        foreach (OperationDescription description in sellerEndPoint.Contract.Operations)
                        {
                            if (description.Name.Equals("query"))
                                description.Behaviors.Add(cacheBehavior);
                        }

                        //intraHost.Description.Behaviors.Add(new ServiceDebugBehavior() { IncludeExceptionDetailInFaults = true });
                        intraHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().InstanceContextMode = InstanceContextMode.Single;
                        intraHost.Description.Behaviors.Find<ServiceBehaviorAttribute>().IncludeExceptionDetailInFaults = true;
                        
                        // Open the service
                        sellerHost.Open();
                        intraHost.Open();



                        // Keeping the service alive till pressing ENTER
                        Console.ReadKey();
                   }
                }
            }

            catch (Exception e)
            {
                Console.WriteLine("Failed executing because:");
                Console.WriteLine(e.Message);
            }
        }
    
    }
}
