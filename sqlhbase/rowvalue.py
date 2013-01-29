__author__ = 'zeph'
__family__ = "mysql"

import os
import sys

def map_hbase(tbl_def, values):
    """HBase friendly values based on tbl_def
     IN: it is supposed to get in 2 arrays: schema, data
         schema: as an array of tuples (column_name, data_type)
    OUT: a tuple (row_id, data_dictionary)
         row_id as a string, or hbase will severely complain
         data_dictionary: { family:column => value }

    >>> map_hbase([('id','int'),('c1','string'),('c2','string')],[1,'v1','v2'])
    ('1', {'mysql:c1': 'v1', 'mysql:c2': 'v2'})
    """
    if len(tbl_def) != len(values):
        raise RuntimeWarning("Mismatching LISTs %s,%s" % (len(tbl_def), len(values)))
    elements = zip(tbl_def, values)
    elements.pop(0) # wasting the ID
    row_key = str(values[0]) # ... since I take it from here
    columns = {}
    for e in elements:
        d,v = e # datatype, value
        k,t = d # key, type
        columns[__family__+":"+k] = str(v)
    return (row_key, columns)

def schema(create_stmt):
    """CREATE stmt to list

    >>> schema(\
        "(`id` int(10) unsigned NOT NULL AUTO_INCREMENT,"+\
        "`vendor_id` int(10) unsigned NOT NULL,"+\
        "`customer_id` int(10) unsigned NOT NULL,"+\
        "`status_id` int(10) unsigned DEFAULT NULL,"+\
        "`status_date` datetime DEFAULT NULL,"+\
        "`paymenttype_id` int(10) unsigned DEFAULT NULL,"+\
        "`payment_status` enum('pending','payed','canceled','error') DEFAULT NULL,"+\
        "`date` datetime DEFAULT NULL,"+\
        "`code` varchar(32) DEFAULT NULL,"+\
        "`customer_comment` mediumtext,"+\
        "`vendor_comment` mediumtext,"+\
        "`order_comment` mediumtext,"+\
        "`expedition_type` enum('delivery','pickup') DEFAULT NULL,"+\
        "`delivery_address_line1` varchar(128) DEFAULT NULL,"+\
        "`delivery_address_line2` varchar(128) DEFAULT NULL,"+\
        "`delivery_address_other` varchar(128) DEFAULT NULL,"+\
        "`delivery_address_postcode` varchar(16) DEFAULT NULL,"+\
        "`delivery_address_number` varchar(32) DEFAULT NULL,"+\
        "`delivery_address_city` varchar(64) DEFAULT NULL,"+\
        "`delivery_address_company` varchar(128) DEFAULT NULL,"+\
        "`delivery_area_id` int(10) unsigned DEFAULT NULL,"+\
        "`total_value` double(18,2) DEFAULT NULL,"+\
        "`subtotal` double(18,2) DEFAULT NULL,"+\
        "`calculated_total` double(18,2) DEFAULT NULL,"+\
        "`service_fee` double(18,2) DEFAULT NULL,"+\
        "`service_fee_total` double(18,2) DEFAULT NULL,"+\
        "`delivery_fee` double(18,2) DEFAULT NULL,"+\
        "`delivery_time` int(11) DEFAULT NULL,"+\
        "`minimum_delivery_value` double(18,2) DEFAULT NULL,"+\
        "`vat` double(18,2) DEFAULT NULL,"+\
        "`free_gift` mediumtext,"+\
        "`commission` double(18,2) DEFAULT NULL,"+\
        "`tracking_identifier` varchar(32) DEFAULT NULL,"+\
        "`tracking_id` int(11) NOT NULL,"+\
        "`_oldsystem_id` int(11) DEFAULT NULL,"+\
        "`_products` mediumtext,"+\
        "`user_id` int(10) unsigned DEFAULT NULL,"+\
        "`edited` tinyint(1) DEFAULT NULL,"+\
        "`source` varchar(128) DEFAULT NULL,"+\
        "`email_feedback` tinyint(1) NOT NULL,"+\
        "`feedback_sent` int(11) NOT NULL DEFAULT '-1' COMMENT '-1:not sent,0:sending,1:sent',"+\
        "`preorder` tinyint(1) unsigned DEFAULT NULL,"+\
        "`expected_delivery_time` varchar(128) DEFAULT NULL,"+\
        "`delivery_charge` tinyint(1) DEFAULT NULL,"+\
        "`service_charge` tinyint(1) DEFAULT NULL,"+\
        "PRIMARY KEY (`id`),"+\
        "KEY `fk_order_2_idx` (`vendor_id`),"+\
        "KEY `fk_order_1_idx` (`customer_id`),"+\
        "KEY `fk_Orders_1_idx` (`status_id`),"+\
        "KEY `fk_Orders_2_idx` (`paymenttype_id`),"+\
        "KEY `fk_Orders_3_idx` (`user_id`),"+\
        "KEY `tracking_identifier` (`tracking_identifier`),"+\
        "KEY `delivery_area_id` (`delivery_area_id`),"+\
        "CONSTRAINT `Orders_customer_id_Customers_id` FOREIGN KEY (`customer_id`) REFERENCES `Customers` (`id`),"+\
        "CONSTRAINT `Orders_ibfk_1` FOREIGN KEY (`delivery_area_id`) REFERENCES `Areas` (`id`),"+\
        "CONSTRAINT `Orders_paymenttype_id_Paymenttypes_id` FOREIGN KEY (`paymenttype_id`) REFERENCES `Paymenttypes` (`id`),"+\
        "CONSTRAINT `Orders_status_id_Status_id` FOREIGN KEY (`status_id`) REFERENCES `Status` (`id`),"+\
        "CONSTRAINT `Orders_user_id_Users_id` FOREIGN KEY (`user_id`) REFERENCES `Users` (`id`),"+\
        "CONSTRAINT `Orders_vendor_id_Vendors_id` FOREIGN KEY (`vendor_id`) REFERENCES `Vendors` (`id`)"+\
        ") ENGINE=InnoDB AUTO_INCREMENT=15994 DEFAULT CHARSET=utf8;")
    [('id', 'int'), \
('vendor_id', 'int'), ('customer_id', 'int'), ('status_id', 'int'), ('status_date', 'datetime'), \
('paymenttype_id', 'int'), ('payment_status', 'enum'), ('date', 'datetime'), ('code', 'varchar'), \
('customer_comment', 'mediumtext'), ('vendor_comment', 'mediumtext'), ('order_comment', 'mediumtext'), \
('expedition_type', 'enum'), ('delivery_address_line1', 'varchar'), ('delivery_address_line2', 'varchar'), \
('delivery_address_other', 'varchar'), ('delivery_address_postcode', 'varchar'), \
('delivery_address_number', 'varchar'), ('delivery_address_city', 'varchar'), ('delivery_address_company', 'varchar'), \
('delivery_area_id', 'int'), ('total_value', 'double'), ('subtotal', 'double'), ('calculated_total', 'double'), \
('service_fee', 'double'), ('service_fee_total', 'double'), ('delivery_fee', 'double'), ('delivery_time', 'int'), \
('minimum_delivery_value', 'double'), ('vat', 'double'), ('free_gift', 'mediumtext'), ('commission', 'double'), \
('tracking_identifier', 'varchar'), ('tracking_id', 'int'), ('_oldsystem_id', 'int'), ('_products', 'mediumtext'), \
('user_id', 'int'), ('edited', 'tinyint'), ('source', 'varchar'), ('email_feedback', 'tinyint'), \
('feedback_sent', 'int'), ('preorder', 'tinyint'), ('expected_delivery_time', 'varchar'), ('delivery_charge', 'tinyint'), \
('service_charge', 'tinyint')]
    """
    tbl_def = []
    idx = create_stmt.rfind(")")
    fields_raw = create_stmt[:idx].strip("(").split(",")
    for field_raw in fields_raw:
        fr = field_raw.split()
        if len(fr) < 2:
            if os.environ.get('DEBUG') is not None:
                print >> sys.stderr, "k,v error:", field_raw
            continue
        if fr[0][0] == "`":
            column = fr[0].strip("`")
            c_type = fr[1].split("(")[0]
            tbl_def.append((column, c_type))
    return tbl_def

if __name__ == '__main__':
    import doctest
    doctest.testmod()
