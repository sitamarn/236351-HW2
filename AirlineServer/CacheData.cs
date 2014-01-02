using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;

namespace AirlineServer
{
    class CacheData
    {
        private const int CACHE_SIZE = 100;
        private static Hashtable hashtable = new Hashtable();
        private static Queue queue = new Queue();
        private static Object cacheLock = new Object();

        private class CasheLine
        {
            public string key;
            public List<Trip> trips;
        }

        public void clear()
        {
            lock (cacheLock)
            {
                hashtable.Clear();
            }
        }


        public void insert(string query, List<Trip> tripList)
        {
            CasheLine line = new CasheLine();
            line.key = query;
            line.trips = tripList;

            lock (cacheLock)
            {
                if (hashtable.Count >= CACHE_SIZE)
                {
                    deleteOldest();
                }

                hashtable.Add(query, line);
                queue.Enqueue(line);
            }
        }

        public List<Trip> getDataFromCache(String query)
        {
            CasheLine result = null;
            String key = query;

            lock (cacheLock)
            {
                if (!hashtable.ContainsKey(key))
                    return null;

                result = (CasheLine)hashtable[key];
            }
           
            return result.trips;
        }


        private void deleteOldest()
        {
            lock (cacheLock)
            {
                while (hashtable.Count >= CACHE_SIZE)
                {
                    CasheLine line = (CasheLine)queue.Dequeue();
                    hashtable.Remove(line.key);
                }
            }
        }

    }
}
