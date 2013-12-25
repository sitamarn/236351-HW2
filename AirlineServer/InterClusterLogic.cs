using Registeration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{

    class InterClusterLogic
    {
        string mUrl;
        string mSellerAddress;
        string mClusterName;
       
        /// <summary>
        /// CTOR
        /// </summary>
        /// <param name="url"></param>
        /// <param name="sellerAddress"></param>
        /// <param name="clusterName"></param>
        public InterClusterLogic(string searchServerURL, string sellerAddress, string clusterName)
        {
            mUrl = searchServerURL;
            mSellerAddress = sellerAddress;
            mClusterName = clusterName;

        }

        /// <summary>
        /// register to the search server as a delegated machine
        /// </summary>
        public void registerAsDelegate()
        {

            try
            {
                WebChannelFactory<IAirSellerRegisteration> cf = new WebChannelFactory<IAirSellerRegisteration>(new Uri(mUrl));
                IAirSellerRegisteration registerChannel = cf.CreateChannel();
                // Register the channel in the server
                registerChannel.RegisterSeller(new Uri(mSellerAddress), mClusterName);

            }
            catch (ProtocolException e)
            {
                Console.WriteLine("Bad Protocol: " + e.Message);
            }
            catch (Exception e)
            {

                if (e.InnerException is WebException)
                {
                    HttpWebResponse resp = (HttpWebResponse)((WebException)e.InnerException).Response;
                    Console.WriteLine("Failed, {0}", resp.StatusDescription);
                }
                else
                {
                    Console.WriteLine("Advertisement connection kicked the bucket, quitting because:");
                    Console.WriteLine(e.Message.ToString());
                }
                return;
            }
        }
    }
}
