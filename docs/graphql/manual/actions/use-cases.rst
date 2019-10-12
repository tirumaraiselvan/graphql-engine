Actions sample use cases
========================


.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:


Actions are ideal for doing custom business logic including data validation, etc.


Data validation
---------------

Suppose you want to insert an article for an author only if they have less than 10 articles.



Sample implementation in HTTP, postgres func, plv8


Complex form data
-----------------

When you have take input in a custom structure or your table models are not best suited for input forms.


Data enrichment
---------------

After performing some custom logic, you may need to return more data to the front-end client. You can do this by creating relationships between actions and your tables.


Custom auth
-----------

Suppose you have an existing auth system which is hard to map to Hasura's permission system. Then, you can deny direct write access to all roles and only allow mutations via actions where you can handle the auth yourself.


