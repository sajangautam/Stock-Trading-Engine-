import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StockTradingEngine {

    private static final int NUM_TICKERS = 1024;
    private final OrderBook[] orderBooks;

    public enum OrderType {
        BUY, SELL
    }

    public StockTradingEngine() {
        orderBooks = new OrderBook[NUM_TICKERS];
        for (int i = 0; i < NUM_TICKERS; i++) {
            orderBooks[i] = new OrderBook();
        }
    }

    public void addOrder(OrderType type, String ticker, int quantity, int price) {
        int index = Math.abs(ticker.hashCode()) % NUM_TICKERS;
        OrderBook orderBook = orderBooks[index];
        System.out.println("Adding Order: " + type + " " + ticker + " Qty: " + quantity + " Price: " + price);
        orderBook.addOrder(type, quantity, price);
        orderBook.matchOrders();
    }

    public static class OrderBook {
        // Atomic head pointers for lock-free linked lists
        private final AtomicReference<Order> buyHead = new AtomicReference<>();
        private final AtomicReference<Order> sellHead = new AtomicReference<>();

        public void addOrder(OrderType type, int quantity, int price) {
            Order newOrder = new Order(type, quantity, price);
            AtomicReference<Order> headRef = (type == OrderType.BUY) ? buyHead : sellHead;
            Order currentHead;
            do {
                currentHead = headRef.get();
                newOrder.next.set(currentHead);
            } while (!headRef.compareAndSet(currentHead, newOrder));
        }

        public void matchOrders() {
            // Clean up fully matched orders before processing
            cleanupOrders(buyHead);
            cleanupOrders(sellHead);

            // Match buy and sell orders
            matchBuyOrders();
            matchSellOrders();
        }

        private void cleanupOrders(AtomicReference<Order> headRef) {
            Order prev = null;
            Order current = headRef.get();
            while (current != null) {
                if (current.quantity.get() <= 0) {
                    if (prev == null) {
                        // Remove head node
                        headRef.compareAndSet(current, current.next.get());
                    } else {
                        // Skip current node
                        prev.next.compareAndSet(current, current.next.get());
                    }
                } else {
                    prev = current;
                }
                current = current.next.get();
            }
        }

        private void matchBuyOrders() {
            Order buyOrder = buyHead.get();
            while (buyOrder != null) {
                int buyRemaining = buyOrder.quantity.get();
                if (buyRemaining <= 0) {
                    buyOrder = buyOrder.next.get();
                    continue;
                }

                Order sellOrder = sellHead.get();
                Order bestSell = null;
                int minPrice = Integer.MAX_VALUE;

                // Find best sell order
                while (sellOrder != null) {
                    int sellRemaining = sellOrder.quantity.get();
                    if (sellRemaining > 0 && sellOrder.price <= buyOrder.price && sellOrder.price < minPrice) {
                        bestSell = sellOrder;
                        minPrice = sellOrder.price;
                    }
                    sellOrder = sellOrder.next.get();
                }

                if (bestSell != null) {
                    int tradeQty = Math.min(buyRemaining, bestSell.quantity.get());
                    if (tradeQty > 0) {
                        // Atomic quantity update
                        boolean buySuccess = buyOrder.quantity.compareAndSet(buyRemaining, buyRemaining - tradeQty);
                        boolean sellSuccess = bestSell.quantity.compareAndSet(bestSell.quantity.get(), bestSell.quantity.get() - tradeQty);

                        if (buySuccess && sellSuccess) {
                            System.out.println("Trade Executed: BUY Order (Qty: " + tradeQty + ", Price: " + buyOrder.price + ") " +
                                    "matched with SELL Order (Qty: " + tradeQty + ", Price: " + bestSell.price + ")");
                        }
                    }
                }
                buyOrder = buyOrder.next.get();
            }
        }

        private void matchSellOrders() {
            Order sellOrder = sellHead.get();
            while (sellOrder != null) {
                int sellRemaining = sellOrder.quantity.get();
                if (sellRemaining <= 0) {
                    sellOrder = sellOrder.next.get();
                    continue;
                }

                Order buyOrder = buyHead.get();
                Order bestBuy = null;
                int maxPrice = Integer.MIN_VALUE;

                // Find best buy order
                while (buyOrder != null) {
                    int buyRemaining = buyOrder.quantity.get();
                    if (buyRemaining > 0 && buyOrder.price >= sellOrder.price && buyOrder.price > maxPrice) {
                        bestBuy = buyOrder;
                        maxPrice = buyOrder.price;
                    }
                    buyOrder = buyOrder.next.get();
                }

                if (bestBuy != null) {
                    int tradeQty = Math.min(sellRemaining, bestBuy.quantity.get());
                    if (tradeQty > 0) {
                        // Atomic quantity update
                        boolean sellSuccess = sellOrder.quantity.compareAndSet(sellRemaining, sellRemaining - tradeQty);
                        boolean buySuccess = bestBuy.quantity.compareAndSet(bestBuy.quantity.get(), bestBuy.quantity.get() - tradeQty);

                        if (sellSuccess && buySuccess) {
                            System.out.println("Trade Executed: SELL Order (Qty: " + tradeQty + ", Price: " + sellOrder.price + ") " +
                                    "matched with BUY Order (Qty: " + tradeQty + ", Price: " + bestBuy.price + ")");
                        }
                    }
                }
                sellOrder = sellOrder.next.get();
            }
        }
    }

    public static class Order {
        final OrderType type;
        final AtomicInteger quantity;
        final int price;
        final AtomicReference<Order> next = new AtomicReference<>();

        public Order(OrderType type, int quantity, int price) {
            this.type = type;
            this.quantity = new AtomicInteger(quantity);
            this.price = price;
        }
    }

    // Simulation Code
    public static void main(String[] args) {
        StockTradingEngine engine = new StockTradingEngine();
        Random rand = new Random();
        String[] tickers = {"AAPL", "MSFT", "NVDA", "GOOG", "META"};

        // Start multiple threads to simulate concurrent orders
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                for (int j = 0; j < 20; j++) {
                    OrderType type = rand.nextBoolean() ? OrderType.BUY : OrderType.SELL;
                    String ticker = tickers[rand.nextInt(tickers.length)];
                    int quantity = rand.nextInt(1000) + 1;
                    int price = rand.nextInt(500) + 100;

                    engine.addOrder(type, ticker, quantity, price);

                    try {
                        Thread.sleep(rand.nextInt(100));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }).start();
        }
    }
}