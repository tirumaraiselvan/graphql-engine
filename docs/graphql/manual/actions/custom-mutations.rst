Create custom mutations using Actions
=====================================


.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:

Actions are user defined mutations with custom business logic. The first step towards creating an action is to define the various types and fields that you want in your schema.

Example
-------

A simple example of creating a user (all scalar input types)

.. code-block:: graphql

   mutation place_order($order_input: place_order_input!) {
     place_order(input: $order_input) {
       action_id
       response {
         order_id
       }
     }
   }



A complicated example of creating an author with articles (few scalar, few nested types)

.. code-block:: graphql

   mutation place_order($order_input: place_order_input!) {
     place_order(input: $order_input) {
       action_id
       response {
         order_id
       }
     }
   }



Input Types
-----------

Reference for creating different input types


.. code-block:: graphql

   enum payment_method {
     stripe
     paytm
   }

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


Response Types
--------------

What all can be returned including relations.


Simple response type

.. code-block:: graphql

   type place_order_response {
     order_id uuid!
   }

Complex reponse type with relations

.. code-block:: graphql

   type place_order_response {
     order_id uuid!
   }


Action Subscriptions
--------------------

Sometimes, your business logic is lengthy or asynchronous. You can take advantage of subscriptions over actions in this case by providing the ``action_id``.

.. code-block:: graphql

   subscription order_status($action_id: uuid!) {
     place_order(action_id: $action_id) {
       order {
         id
         payment_url
         total_amount
         discount
       }
     }
   }


