-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Jul 26, 2025 at 04:05 PM
-- Server version: 10.4.27-MariaDB
-- PHP Version: 8.1.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `constra`
--

-- --------------------------------------------------------

--
-- Table structure for table `exchanges`
--

CREATE TABLE `exchanges` (
  `id` int(11) NOT NULL,
  `exchange_name` varchar(255) NOT NULL,
  `requirePass` int(11) NOT NULL DEFAULT 0,
  `status` int(11) NOT NULL DEFAULT 1,
  `date_added` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `exchanges`
--

INSERT INTO `exchanges` (`id`, `exchange_name`, `requirePass`, `status`, `date_added`) VALUES
(1, 'phemex', 0, 1, '2025-07-19 22:58:04');

-- --------------------------------------------------------

--
-- Table structure for table `hedge_limit`
--

CREATE TABLE `hedge_limit` (
  `id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `trade_id` int(11) NOT NULL,
  `order_id` varchar(255) NOT NULL,
  `trade_type` int(11) NOT NULL,
  `status` int(11) NOT NULL,
  `date_added` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `hedge_limit`
--

INSERT INTO `hedge_limit` (`id`, `user_id`, `trade_id`, `order_id`, `trade_type`, `status`, `date_added`) VALUES
(21, 1, 13, '44e75f59-8f96-4530-a04e-652a6ce0041d', 0, 1, '0000-00-00 00:00:00'),
(22, 1, 18, '3ee01dd1-2252-4501-b83f-99cbc2bf56d6', 0, 1, '0000-00-00 00:00:00');

-- --------------------------------------------------------

--
-- Table structure for table `opn_trade`
--

CREATE TABLE `opn_trade` (
  `id` int(11) NOT NULL,
  `strategy_type` varchar(255) NOT NULL,
  `user_cred_id` int(11) NOT NULL,
  `child_to` int(11) NOT NULL,
  `trade_signal` int(11) NOT NULL,
  `order_id` varchar(255) NOT NULL,
  `symbol` varchar(255) NOT NULL,
  `trade_type` int(11) NOT NULL,
  `amount` double(16,3) DEFAULT 0.000,
  `lv_size` double(16,3) DEFAULT 0.000,
  `allow_rentry` int(20) DEFAULT NULL,
  `leverage` int(11) NOT NULL,
  `trail_order_id` varchar(255) DEFAULT NULL,
  `trail_threshold` double(16,4) DEFAULT 0.0000,
  `profit_target_distance` double(16,4) DEFAULT 0.0000,
  `cum_close_threshold` double(16,6) NOT NULL,
  `cum_close_distance` double(16,6) NOT NULL,
  `to_liquidation_order_id` varchar(255) DEFAULT NULL,
  `trade_done` tinyint(4) DEFAULT 0,
  `re_entry_count` int(11) DEFAULT NULL,
  `dn_allow_rentry` int(11) NOT NULL DEFAULT 0,
  `hedged` int(11) NOT NULL,
  `hedge_start` int(11) NOT NULL,
  `hedge_limit` int(11) NOT NULL,
  `execSeq` varchar(130) NOT NULL,
  `floating_pnl` double(16,8) NOT NULL,
  `realizedPnl` double(16,8) NOT NULL,
  `prevCumClosedPnl` double(16,8) NOT NULL,
  `currCumClosedPnl` double(16,8) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT 1,
  `date_added` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `opn_trade`
--

INSERT INTO `opn_trade` (`id`, `strategy_type`, `user_cred_id`, `child_to`, `trade_signal`, `order_id`, `symbol`, `trade_type`, `amount`, `lv_size`, `allow_rentry`, `leverage`, `trail_order_id`, `trail_threshold`, `profit_target_distance`, `cum_close_threshold`, `cum_close_distance`, `to_liquidation_order_id`, `trade_done`, `re_entry_count`, `dn_allow_rentry`, `hedged`, `hedge_start`, `hedge_limit`, `execSeq`, `floating_pnl`, `realizedPnl`, `prevCumClosedPnl`, `currCumClosedPnl`, `status`, `date_added`) VALUES
(1, 'initial', 1, 0, -10, '1_NEWT/USDT:USDT_live_sell', 'NEWT/USDT:USDT', 1, 1.600, 3.200, NULL, -10, '1cc5f908-1cf0-4a8f-9230-c752e58e7628', 2.4000, 0.2000, 0.000000, 0.000000, NULL, 1, 0, 1, 0, 3, 0, '2986403849', 0.00000000, 0.13872000, 0.00000000, 0.13872000, -2, '2025-07-24 08:48:46'),
(2, 'initial', 1, 0, -10, '1_VINE/USDT:USDT_live_sell', 'VINE/USDT:USDT', 1, 2.000, 2.000, NULL, -10, 'e25ef982-9d5f-46eb-ab29-338b3bef003c', 1.5000, 0.0500, 0.000000, 0.000000, NULL, 1, -1, 1, 0, 3, 0, '7360057862', 0.00000000, 0.01014000, 0.00000000, 0.01014000, -2, '2025-07-25 02:09:48'),
(3, 'initial', 1, 0, -10, '1_AXL/USDT:USDT_live_sell', 'AXL/USDT:USDT', 1, 1.000, 1.000, NULL, 5, NULL, 0.1000, 0.0500, 0.000000, 0.000000, NULL, 0, 0, 1, 0, 3, 0, '10255813094', 0.00000000, 0.00000000, 0.63940000, 0.00000000, 1, '2025-07-25 08:49:36'),
(4, 'initial', 1, 0, -10, '1_SAROS/USDT:USDT_live_sell', 'SAROS/USDT:USDT', 1, 1.000, 1.000, NULL, 10, NULL, 0.1000, 0.0500, 0.000000, 0.000000, NULL, 0, 0, 1, 0, 3, 0, '2647802803', 0.00000000, 0.00000000, 0.11684000, 0.00000000, 1, '2025-07-25 16:33:22'),
(5, 'initial', 1, 0, -10, '1_ENA/USDT:USDT_live_sell', 'ENA/USDT:USDT', 1, 1.000, 1.000, NULL, 5, NULL, 0.1000, 0.0500, 0.000000, 0.000000, NULL, 0, 0, 1, 0, 3, 0, '18852504805', 0.00000000, 0.00000000, 0.04980000, 0.00000000, 1, '2025-07-25 22:50:04');

--
-- Triggers `opn_trade`
--
DELIMITER $$
CREATE TRIGGER `opn_trade_after_delete` AFTER DELETE ON `opn_trade` FOR EACH ROW BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('delete', 'opn_trade', OLD.id, OLD.user_cred_id);
        END
$$
DELIMITER ;
DELIMITER $$
CREATE TRIGGER `opn_trade_after_insert` AFTER INSERT ON `opn_trade` FOR EACH ROW BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('insert', 'opn_trade', NEW.id, NEW.user_cred_id);
        END
$$
DELIMITER ;
DELIMITER $$
CREATE TRIGGER `opn_trade_after_update` AFTER UPDATE ON `opn_trade` FOR EACH ROW BEGIN
            INSERT INTO redis_notify (action_type, table_name, row_id, user_cred_id)
            VALUES ('update', 'opn_trade', NEW.id, NEW.user_cred_id);
        END
$$
DELIMITER ;

-- --------------------------------------------------------

--
-- Table structure for table `trade_signal`
--

CREATE TABLE `trade_signal` (
  `id` int(11) NOT NULL,
  `exchange` int(11) NOT NULL,
  `symbol_pair` varchar(255) NOT NULL,
  `trade_type` int(11) NOT NULL,
  `status` int(11) NOT NULL DEFAULT 1,
  `date_added` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `user_cred`
--

CREATE TABLE `user_cred` (
  `id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `exchange_id` int(11) NOT NULL,
  `api_key` varchar(255) NOT NULL,
  `secret` varchar(255) NOT NULL,
  `password` varchar(200) DEFAULT NULL,
  `status` int(11) NOT NULL DEFAULT 1,
  `date_added` datetime DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `user_cred`
--

INSERT INTO `user_cred` (`id`, `user_id`, `exchange_id`, `api_key`, `secret`, `password`, `status`, `date_added`) VALUES
(1, 1, 1, 'c895dd8c-1cc1-4e88-86a2-b6e856c274ba', 'qO73E8E6SsLlQ4TDgjhGpe-h7KWZYYhdNHl-bYLW0woyOTNiNWFlYi1lYmJmLTRmOWEtYTIxOS1hY2RkMjk3YzM5MjI', NULL, 1, '2025-07-19 22:56:37');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `exchanges`
--
ALTER TABLE `exchanges`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `hedge_limit`
--
ALTER TABLE `hedge_limit`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `opn_trade`
--
ALTER TABLE `opn_trade`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `trade_signal`
--
ALTER TABLE `trade_signal`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `unique_exchange_symbol` (`symbol_pair`);

--
-- Indexes for table `user_cred`
--
ALTER TABLE `user_cred`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `exchanges`
--
ALTER TABLE `exchanges`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `hedge_limit`
--
ALTER TABLE `hedge_limit`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=23;

--
-- AUTO_INCREMENT for table `opn_trade`
--
ALTER TABLE `opn_trade`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;

--
-- AUTO_INCREMENT for table `trade_signal`
--
ALTER TABLE `trade_signal`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `user_cred`
--
ALTER TABLE `user_cred`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;