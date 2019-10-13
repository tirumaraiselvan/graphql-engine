Action Input Types
==================

.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:


Reference for creating different input types


Scalar Input Types
------------------

.. code-block:: graphql

   enum payment_method {
     stripe
     paytm
   }


Object Input Types
------------------

.. code-block:: graphql

   input type place_order_input {
     selected_payment_mode payment_method!
     items [order_item_input!]!
     address_id uuid!
     coupon_code String
   }

   input order_item_input {
     skuId uuid!
     quantity Int!
   }

