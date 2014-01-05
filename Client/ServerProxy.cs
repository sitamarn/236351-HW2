using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ServiceModel.Web;
using System.Globalization;
using System.ServiceModel;
using System.Net;
using FlightSearchServer;

namespace Client
{


    class ServerProxy
    {
        WebChannelFactory<FlightSearchServer.IClientQueryService>
            cf;

        Uri serverUri;
        FlightSearchServer.IClientQueryService channel;

        private static DateTime GetDate(string strDate)
        {
            DateTime date = DateTime.ParseExact(strDate, "dd/MM/yyyy", CultureInfo.InvariantCulture);
            return date;
        }

        public ServerProxy(string uri)
        {
            serverUri = new Uri(uri);
            Console.WriteLine("Connection to: {0}", uri);
            cf = new WebChannelFactory<FlightSearchServer.IClientQueryService>(serverUri);
            channel = cf.CreateChannel();
        }

        public void run()
        {
            try
            {
                executionLoop();
            }
            catch (EndpointNotFoundException)
            {
                Console.WriteLine("Ticket Selling Server is down");
            }
            catch (Exception e)
            {
                Console.WriteLine("{0}", e.Message.ToString());
                Console.WriteLine("=========================================");
                Console.WriteLine("{0}", e.GetType());
            }
        }

        private void executionLoop()
        {
            Console.WriteLine("Enter a command or exit to quit");
            do
            {
                Console.Write(">");
                string line = Console.ReadLine();
                if (line != null)
                {
                    string[] parameters = line.Split(' ');
                    string command = parameters[0].ToLower();

                    try
                    {
                        if (command.Equals(""))
                        {
                            continue;
                        }
                        else if (command.Equals("search"))
                        {
                            search(parameters);
                        }
                        else if (command.Equals("exit"))
                        {
                            return;
                        }
                        else
                        {
                            Console.WriteLine("Invalid command");
                        }
                    }
                    catch (NullReferenceException)
                    {
                        Console.WriteLine("Failed creating WCF Object");
                        break;
                    }
                    catch (FormatException)
                    {
                        Console.WriteLine("Invalid time format, use dd/MM/yyyy format");
                    }
                    catch (EndpointNotFoundException e)
                    {
                        Console.WriteLine("search service in the FlightSearchServer is down: "+e.Message);
                    }
                    catch (Exception e)
                    {
                        if (e.InnerException is WebException)
                        {
                            HttpWebResponse resp = (HttpWebResponse)((WebException)e.InnerException).Response;
                            Console.WriteLine("service responded with " + "code: {0}, description: {1}", resp.StatusCode, resp.StatusDescription);

                        }
                        else
                        {
                            Console.WriteLine("search server failed:");
                            Console.WriteLine(e.Message.ToString());
                        }
                    }
                }
            } while (true);
        }

        void search(string[] input)
        {
            if (input.Length < 3)
            {
                Console.WriteLine("Invalid parameters");
                Console.WriteLine("search <src> <dst> <dd/MM/yyyy> <Airline Server 1> ... <Airline Server N>");
            }
            else
            {
                string src = input[1];
                string dst = input[2];
                string strDate = input[3];

                String airServers = "";

                for (int i = 4; i < input.Length; i++)
                {
                    airServers += (input[i] + ",");
                }
                if (!airServers.Equals(""))
                {
                    airServers = airServers.Remove(airServers.Length - 1);
                }
                QueryResultTrips result = null;
                GetDate(strDate);

                result = channel.GetFlights(src, dst, strDate, airServers);

                foreach (QueryResultTrip trip in result)
                {
                    Console.Write("{0}$: ", trip.price);

                    Console.Write("{0}-{1} ({2}, {3})", trip.firstFlight.src, trip.firstFlight.dst, trip.firstFlight.seller, trip.firstFlight.flightNumber);
                    if (trip.secondFlight != null)
                    {
                        Console.Write(", {0}-{1} ({2}, {3})", trip.firstFlight.src, trip.firstFlight.dst, trip.firstFlight.seller, trip.firstFlight.flightNumber);
                    }
                    Console.WriteLine();
                }
            }
        }
    }
}
